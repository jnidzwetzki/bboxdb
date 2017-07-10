/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.distribution.zookeeper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.DistributedInstanceManager;
import org.bboxdb.distribution.membership.event.DistributedInstanceState;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.util.ServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperClient implements BBoxDBService, Watcher {

	/**
	 * The list of the zookeeper hosts
	 */
	protected final Collection<String> zookeeperHosts;

	/**
	 * The name of the bboxdb cluster
	 */
	protected final String clustername;

	/**
	 * The zookeeper client instance
	 */
	protected ZooKeeper zookeeper;

	/**
	 * Is the membership observer active?
	 */
	protected volatile boolean membershipObserver = false;

	/**
	 * Service state
	 */
	protected final ServiceState serviceState;

	/**
	 * The timeout for the zookeeper session in miliseconds
	 */
	protected final static int ZOOKEEPER_SESSION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

	/**
	 * The connect timeout in seconds
	 */
	protected final static int ZOOKEEPER_CONNCT_TIMEOUT = 30;
	
	/**
	 * The after connect callbacks
	 */
	protected final List<Consumer<ZookeeperClient>> afterConnectCallbacks;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

	public ZookeeperClient(final Collection<String> zookeeperHosts, final String clustername) {
		this.zookeeperHosts = zookeeperHosts;
		this.clustername = clustername;
		this.serviceState = new ServiceState();
		this.afterConnectCallbacks = new ArrayList<Consumer<ZookeeperClient>>();
	}

	/**
	 * Connect to zookeeper
	 */
	@Override
	public void init() {
		if (zookeeperHosts == null || zookeeperHosts.isEmpty()) {
			logger.warn("No Zookeeper hosts are defined, not connecting to zookeeper");
			return;
		}

		try {
			serviceState.reset();
			serviceState.dipatchToStarting();

			final CountDownLatch connectLatch = new CountDownLatch(1);

			zookeeper = new ZooKeeper(generateConnectString(), ZOOKEEPER_SESSION_TIMEOUT, new Watcher() {
				@Override
				public void process(final WatchedEvent event) {
					if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
						connectLatch.countDown();
					}
				}
			});

			final boolean waitResult = connectLatch.await(ZOOKEEPER_CONNCT_TIMEOUT, TimeUnit.SECONDS);

			if (waitResult == false) {
				logger.warn("Unable to connect in " + ZOOKEEPER_CONNCT_TIMEOUT + " seconds");
				closeZookeeperConnectionNE();
				return;
			}

			createDirectoryStructureIfNeeded();
			
			// Notify the callbacks
			afterConnectCallbacks.forEach(c -> c.accept(this));
			
			serviceState.dispatchToRunning();
		} catch (Exception e) {
			logger.warn("Got exception while connecting to zookeeper", e);
		}
	}

	/**
	 * Disconnect from zookeeper
	 * 
	 * This method is synchronized to guarantee that all running callbacks are
	 * completed before the shutdown is executed
	 */
	@Override
	public synchronized void shutdown() {

		if (!serviceState.isInRunningState()) {
			logger.warn("Unable to shutdown, service is in {} state", serviceState);
			return;
		}

		serviceState.dispatchToStopping();
		stopMembershipObserver();
		closeZookeeperConnectionNE();
		serviceState.dispatchToTerminated();
	}

	/**
	 * Close the zookeeper connection without any exception
	 */
	protected void closeZookeeperConnectionNE() {
		if (zookeeper == null) {
			return;
		}

		try {
			logger.info("Disconnecting from zookeeper");
			zookeeper.close();
		} catch (InterruptedException e) {
			logger.warn("Got exception while closing zookeeper connection", e);
			Thread.currentThread().interrupt();
		}

		zookeeper = null;
	}

	/**
	 * Start the membership observer
	 * 
	 * @return
	 */
	public boolean startMembershipObserver() {

		if (zookeeper == null) {
			logger.error("startMembershipObserver() called before init()");
			return false;
		}

		membershipObserver = true;
		readMembershipAndRegisterWatch();

		return true;
	}

	/**
	 * Stop the membership observer. This will send a delete event for all known
	 * instances if the membership observer was active.
	 */
	public void stopMembershipObserver() {

		if (membershipObserver == true) {
			final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
			distributedInstanceManager.zookeeperDisconnect();
		}

		membershipObserver = false;
	}

	/**
	 * Register a watch on membership changes. A watch is a one-time operation,
	 * the watch is reregistered on each method call.
	 */
	protected boolean readMembershipAndRegisterWatch() {

		if (!membershipObserver) {
			logger.info("Ignore membership event, because observer is not active");
			return false;
		}

		try {
			final DistributedInstanceManager distributedInstanceManager 
				= DistributedInstanceManager.getInstance();

			// Reregister watch on membership
			final String activeNodesPath = getActiveInstancesPath();
			zookeeper.getChildren(activeNodesPath, this);

			// Read version data
			final String detailsPath = getDetaisPath();
			final List<String> instances = zookeeper.getChildren(detailsPath, null);

			final Set<DistributedInstance> instanceSet = new HashSet<>();
			
			for (final String instance : instances) {
				final DistributedInstance theInstance = new DistributedInstance(instance);

				final String instanceVersion = getVersionForInstance(theInstance);
				theInstance.setVersion(instanceVersion);
				
				final DistributedInstanceState state = getStateForInstance(instance);
				theInstance.setState(state);
				
				instanceSet.add(theInstance);
			}

			distributedInstanceManager.updateInstanceList(instanceSet);
		} catch (KeeperException | ZookeeperNotFoundException e) {
			logger.warn("Unable to read membership and create a watch", e);
			return false;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Unable to read membership and create a watch", e);
			return false;
		}

		return true;
	}

	/**
	 * Read the state for the given instance
	 * 
	 * @param member
	 * @return
	 * @throws ZookeeperNotFoundException
	 */
	protected DistributedInstanceState getStateForInstance(final String member) {
		final String nodesPath = getActiveInstancesPath();
		final String statePath = nodesPath + "/" + member;

		try {
			final String state = readPathAndReturnString(statePath, true, this);
			if (DistributedInstanceState.OUTDATED.getZookeeperValue().equals(state)) {
				return DistributedInstanceState.OUTDATED;
			} else if (DistributedInstanceState.READY.getZookeeperValue().equals(state)) {
				return DistributedInstanceState.READY;
			}
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			// Ignore exception, instance state is unknown
		}

		return DistributedInstanceState.UNKNOWN;
	}

	/**
	 * Read the version for the given instance
	 * 
	 * @param member
	 * @return
	 * @throws ZookeeperNotFoundException
	 */
	protected String getVersionForInstance(final DistributedInstance member) throws ZookeeperNotFoundException {
		final String versionPath = getInstancesVersionPath(member);

		try {
			return readPathAndReturnString(versionPath, true, null);
		} catch (ZookeeperException e) {
			logger.info("Unable to read version for: {}", versionPath);
		}

		return DistributedInstance.UNKOWN_VERSION;
	}

	/**
	 * Get the children and register a watch
	 * 
	 * @param path
	 * @param watcher
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public List<String> getChildren(final String path, final Watcher watcher)
			throws ZookeeperException, ZookeeperNotFoundException {

		try {

			if (zookeeper.exists(path, false) == null) {
				return null;
			}

			return zookeeper.getChildren(path, watcher);
		} catch (KeeperException e) {

			// Was node deleted between exists and getData call?
			if (e.code() == Code.NONODE) {
				throw new ZookeeperNotFoundException("The path does not exist: " + path, e);
			} else {
				throw new ZookeeperException(e);
			}

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Build a comma separated list of the zookeeper nodes
	 * 
	 * @return
	 */
	protected String generateConnectString() {

		// No zookeeper hosts are defined
		if (zookeeperHosts == null) {
			logger.warn("No zookeeper hosts are defined");
			return "";
		}

		final StringBuilder sb = new StringBuilder();

		for (final String zookeeperHost : zookeeperHosts) {

			if (sb.length() != 0) {
				sb.append(",");
			}

			sb.append(zookeeperHost);
		}

		return sb.toString();
	}

	/**
	 * Zookeeper watched event
	 */
	@Override
	public void process(final WatchedEvent watchedEvent) {

		try {
			logger.debug("Got zookeeper event: {} " + watchedEvent);
			processZookeeperEvent(watchedEvent);
		} catch (Throwable e) {
			logger.error("Got uncought exception while processing event", e);
		}

	}

	/**
	 * Process zooekeeper events
	 * 
	 * @param watchedEvent
	 */
	protected synchronized void processZookeeperEvent(final WatchedEvent watchedEvent) {
		// Ignore null parameter
		if (watchedEvent == null) {
			logger.warn("process called with an null argument");
			return;
		}

		// Shutdown is pending, stop event processing
		if (!serviceState.isInRunningState()) {
			logger.debug("Ignoring event {}, because service state is {}", watchedEvent, serviceState);
			return;
		}

		// Ignore type=none event
		if (watchedEvent.getType() == EventType.None) {
			return;
		}

		// Process events
		if (watchedEvent.getPath() != null) {
			if (watchedEvent.getPath().startsWith(getInstancesPath())) {
				readMembershipAndRegisterWatch();
			}
		} else {
			logger.warn("Got unknown zookeeper event: {}", watchedEvent);
		}
	}

	/**
	 * Write the given data to zookeeper
	 * 
	 * @param path
	 * @param value
	 * @throws ZookeeperException
	 */
	public boolean setData(final String path, final String value) throws ZookeeperException {
		return setData(path, value, -1);
	}

	/**
	 * Write the given data to zookeeper if the version matches
	 * 
	 * @param path
	 * @param value
	 * @throws ZookeeperException
	 */
	public boolean setData(final String path, final String value, final int version) throws ZookeeperException {

		try {
			zookeeper.setData(path, value.getBytes(), -1);

			return true;
		} catch (KeeperException e) {

			// Version does not match
			if (e.code() == Code.BADVERSION) {
				return false;
			}

			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Read the data and the stat from the given path
	 * 
	 * @throws ZookeeperException
	 */
	public String getData(final String path, final Stat stat) throws ZookeeperException {
		try {
			return new String(zookeeper.getData(path, false, stat));
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Read the data from the given path
	 * 
	 * @param path
	 * @return
	 * @throws ZookeeperException
	 */
	public String getData(final String path) throws ZookeeperException {
		return getData(path, null);
	}

	/**
	 * Set the state for the local instance
	 * 
	 * @param distributedInstanceState
	 * @return
	 * @throws ZookeeperException
	 */
	public boolean setLocalInstanceState(final DistributedInstance instance, 
			final DistributedInstanceState distributedInstanceState)
			throws ZookeeperException {
		
		if (instance == null) {
			logger.warn("Try to set instance state without register local instance: " + distributedInstanceState);
			return false;
		}

		final String statePath = getActiveInstancesPath() + "/" + instance.getStringValue();

		try {
			zookeeper.setData(statePath, distributedInstanceState.getZookeeperValue().getBytes(), -1);
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}

		return true;
	}

	/**
	 * Register the name of the cluster in the zookeeper directory
	 * 
	 * @throws ZookeeperException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	protected void createDirectoryStructureIfNeeded() throws ZookeeperException {

		// Active instances
		final String activeInstancesPath = getActiveInstancesPath();
		createDirectoryStructureRecursive(activeInstancesPath);

		// Details of the instances
		final String detailsPath = getDetaisPath();
		createDirectoryStructureRecursive(detailsPath);
	}

	/**
	 * Get the path of the zookeeper clustername
	 * 
	 * @param clustername
	 * @return
	 */
	public String getClusterPath() {
		return "/" + clustername;
	}

	/**
	 * Get the path for the systems
	 * 
	 * @param clustername
	 * @return
	 */
	protected String getInstancesPath() {
		return getClusterPath() + "/" + ZookeeperNodeNames.NAME_SYSTEMS;
	}

	/**
	 * Get the path of the zookeeper nodes
	 */
	protected String getActiveInstancesPath() {
		return getInstancesPath() + "/active";
	}

	/**
	 * Get the details path
	 * @return
	 */
	protected String getDetaisPath() {
		return getInstancesPath() + "/details";
	}
	
	/**
	 * Get the node info path
	 * @param distributedInstance 
	 * @return
	 */
	protected String getInstanceDetailsPath(final DistributedInstance distributedInstance) {
		return getDetaisPath() + "/" + distributedInstance.getStringValue();
	}
	
	/**
	 * Get the path of the version node
	 */
	protected String getInstancesVersionPath(final DistributedInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/version";
	}
	

	/**
	 * Get the path of the cpu core node
	 */
	protected String getInstancesCpuCorePath(final DistributedInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/cpucore";
	}
	
	/**
	 * Get the path of the memory node
	 */
	protected String getInstancesMemoryPath(final DistributedInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/memory";
	}
	
	/**
	 * Get the path of the diskspace node
	 */
	protected String getInstancesDiskspacePath(final DistributedInstance distributedInstance) {
		return getInstanceDetailsPath(distributedInstance) + "/diskspace";
	}

	/**
	 * Get the free space of the diskspace node
	 */
	protected String getInstancesDiskspaceFreePath(final DistributedInstance distributedInstance, 
			final String path) {
		return getInstancesDiskspacePath(distributedInstance) + "/" + path + "/free";
	}
	
	/**
	 * Get the total space of the diskspace node
	 */
	protected String getInstancesDiskspaceTotalPath(final DistributedInstance distributedInstance, 
			final String path) {
		return getInstancesDiskspacePath(distributedInstance) + "/" + path + "/total";
	}
	
	@Override
	public String getServicename() {
		return "Zookeeper Client";
	}

	/**
	 * Create the given directory structure recursive
	 * 
	 * @param path
	 * @throws ZookeeperException
	 */
	public void createDirectoryStructureRecursive(final String path) throws ZookeeperException {

		try {
			// Does the full path already exists?
			if (zookeeper.exists(path, false) != null) {
				return;
			}

			// Otherwise check and create all sub paths
			final String[] allNodes = path.split("/");
			final StringBuilder sb = new StringBuilder();

			// Start by 1 to skip the initial /
			for (int i = 1; i < allNodes.length; i++) {
				final String nextNode = allNodes[i];
				sb.append("/");
				sb.append(nextNode);

				final String partialPath = sb.toString();

				if (zookeeper.exists(partialPath, false) == null) {
					logger.info("Path '{}' not found, creating", partialPath);

					zookeeper.create(partialPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			}
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Read the given path and returns a string result
	 * 
	 * @param pathName
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public String readPathAndReturnString(final String pathName, final boolean retry, final Watcher watcher)
			throws ZookeeperException, ZookeeperNotFoundException {

		try {
			if (zookeeper.exists(pathName, false) == null) {
				if (retry != true) {
					throw new ZookeeperNotFoundException("The path does not exist: " + pathName);
				} else {
					Thread.sleep(500);

					if (zookeeper.exists(pathName, false) == null) {
						throw new ZookeeperNotFoundException("The path does not exist: " + pathName);
					}
				}
			}

			final byte[] bytes = zookeeper.getData(pathName, watcher, null);
			return new String(bytes);
		} catch (KeeperException e) {

			// Was node deleted between exists and getData call?
			if (e.code() == Code.NONODE) {
				throw new ZookeeperNotFoundException("The path does not exist: " + pathName, e);
			} else {
				throw new ZookeeperException(e);
			}

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Dies the given path exists?
	 * 
	 * @param pathName
	 * @return
	 * @throws ZookeeperException
	 */
	public boolean exists(final String pathName) throws ZookeeperException {
		try {
			if (zookeeper.exists(pathName, false) != null) {
				return true;
			}
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}

		return false;
	}

	/**
	 * Delete the node recursive
	 * 
	 * @param path
	 * @throws ZookeeperException
	 */
	public void deleteNodesRecursive(final String path) throws ZookeeperException {
		try {

			final List<String> childs = zookeeper.getChildren(path, false);

			for (final String child : childs) {
				deleteNodesRecursive(path + "/" + child);
			}

			zookeeper.delete(path, -1);

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.NONODE) {
				// We try to delete concurrently deleted
				// nodes. So we can ignore the exception.
			} else {
				throw new ZookeeperException(e);
			}
		}
	}

	/**
	 * Delete all known data about a cluster
	 * 
	 * @param distributionGroup
	 * @throws ZookeeperException
	 */
	public void deleteCluster() throws ZookeeperException {
		stopMembershipObserver();

		final String path = getClusterPath();
		deleteNodesRecursive(path);
	}

	/**
	 * Create a new persistent node
	 * 
	 * @param path
	 * @param bytes
	 * @return
	 * @throws ZookeeperException
	 */
	public String createPersistentNode(final String path, final byte[] bytes) throws ZookeeperException {
		try {
			return zookeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Create a new persistent sequential node
	 * 
	 * @param path
	 * @param bytes
	 * @return
	 * @throws ZookeeperException
	 */
	public String createPersistentSequencialNode(final String path, final byte[] bytes) throws ZookeeperException {
		try {
			return zookeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Replace the persistent node
	 * @param path
	 * @param bytes
	 * @throws ZookeeperException 
	 */
	public void replacePersistentNode(final String path, final byte[] bytes) throws ZookeeperException {
		try {
			if (zookeeper.exists(path, false) != null) {
				zookeeper.setData(path, bytes, -1);
			} else {
				createDirectoryStructureRecursive(path);
				zookeeper.setData(path, bytes, -1);
			}
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Replace the ephemeral node
	 * @param path
	 * @param bytes
	 * @throws ZookeeperException
	 */
	public void replaceEphemeralNode(final String path, final byte[] bytes) throws ZookeeperException {
		
		try {
			// Delete old state if exists (e.g. caused by a fast restart of the
			// service)
			if (zookeeper.exists(path, false) != null) {
				zookeeper.delete(path, -1);
			}
	
			// Register new state
			zookeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			throw new ZookeeperException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZookeeperException(e);
		}
	}

	/**
	 * Replace the value for the given path, only if the old value matches. This
	 * operation is performed atomic.
	 * 
	 * @param path
	 * @param oldValue
	 * @param newValue
	 * @return
	 * @throws ZookeeperException
	 */
	public boolean testAndReplaceValue(final String path, final String oldValue, final String newValue)
			throws ZookeeperException {

		if (oldValue == null || newValue == null) {
			throw new IllegalArgumentException("Invalid parameter null for old or new value");
		}

		if (!exists(path)) {
			logger.debug("Unable to replace value, path {} does not exists", path);
			return false;
		}

		// Retry value assignment
		for (int retry = 0; retry < 10; retry++) {
			final Stat stat = new Stat();
			final String zookeeperValue = getData(path, stat);

			// Old value does not match
			if (!oldValue.equals(zookeeperValue)) {
				logger.debug("Unable to replace value, zk value {} for path {} does not match expected value {}",
						zookeeperValue, path, oldValue);

				return false;
			}

			// Replace only if version of the read matches
			final boolean result = setData(path, newValue, stat.getVersion());

			if (result == true) {
				return true;
			}
		}

		logger.debug("Unable to replace {} with {} in path", oldValue, newValue, path);

		return false;
	}

	/**
	 * Is the zookeeper client connected?
	 * 
	 * @return
	 */
	public boolean isConnected() {
		if (zookeeper == null) {
			return false;
		}

		return zookeeper.getState() == States.CONNECTED;
	}

	/**
	 * Returns the name of the cluster
	 * 
	 * @return
	 */
	public String getClustername() {
		return clustername;
	}
		
	/** 
	 * Get the zookeeper client instance
	 */
	public ZooKeeper getZookeeper() {
		return zookeeper;
	}
	
	/**
	 * Register a after connect callback
	 * @param callback
	 */
	public void registerAfterConnectCallback(final Consumer<ZookeeperClient> callback) {
		afterConnectCallbacks.add(callback);
	}

}
/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.concurrent.AcquirableRessource;
import org.bboxdb.misc.BBoxDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperClient implements BBoxDBService, AcquirableRessource {

	/**
	 * The list of the zookeeper hosts
	 */
	protected final String connectionString;

	/**
	 * The name of the bboxdb cluster
	 */
	protected final String clustername;

	/**
	 * The zookeeper client instance
	 */
	protected ZooKeeper zookeeper;
	
	/**
	 * Service state
	 */
	protected final ServiceState serviceState;
	
	/**
	 * The usage counter
	 */
	protected Phaser usage;

	/**
	 * The timeout for the zookeeper session in milliseconds
	 */
	protected final static int ZOOKEEPER_SESSION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

	/**
	 * The connect timeout in seconds
	 */
	protected final static int ZOOKEEPER_CONNECT_TIMEOUT_IN_SEC = 5;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

	public ZookeeperClient(final Collection<String> zookeeperHosts, final String clustername) {
		
		Objects.requireNonNull(zookeeperHosts);
		
		if (zookeeperHosts.isEmpty()) {
			throw new IllegalArgumentException("No Zookeeper hosts are defined");
		}
		
		this.connectionString = zookeeperHosts.stream().collect(Collectors.joining(","));
		this.clustername = Objects.requireNonNull(clustername);
		this.serviceState = new ServiceState();
	}

	/**
	 * Connect to zookeeper
	 */
	@Override
	public void init() {

		try {
			serviceState.reset();
			serviceState.dipatchToStarting();
			usage = new Phaser(1);

			final CountDownLatch connectLatch = new CountDownLatch(1);
			
			zookeeper = new ZooKeeper(connectionString, ZOOKEEPER_SESSION_TIMEOUT, new Watcher() {
				@Override
				public void process(final WatchedEvent event) {
					if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
						connectLatch.countDown();
					}
				}
			});

			final boolean waitResult = connectLatch.await(ZOOKEEPER_CONNECT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);

			if (waitResult == false) {
				throw new ZookeeperException("Unable to connect in " + ZOOKEEPER_CONNECT_TIMEOUT_IN_SEC 
						+" seconds. Connect string is: " + connectionString);
			}

			createDirectoryStructureIfNeeded();
			
			serviceState.dispatchToRunning();
		} catch (Exception e) {
			logger.warn("Got exception while connecting to zookeeper", e);
			closeZookeeperConnectionNE();
			serviceState.dispatchToFailed(e);
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

		if (! serviceState.isInRunningState()) {
			logger.warn("Unable to shutdown, service is in {} state", serviceState);
			return;
		}

		serviceState.dispatchToStopping();
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
			
			// Wait until nobody uses the instance
			assert (! usage.isTerminated()) : "Usage counter is terminated";
			usage.arriveAndAwaitAdvance();
			
			zookeeper.close();
		} catch (InterruptedException e) {
			logger.warn("Got exception while closing zookeeper connection", e);
			Thread.currentThread().interrupt();
		}

		zookeeper = null;
	}
	
	/**
	 * Get the children and register without creating a watch
	 */
	public List<String> getChildren(final String path)
			throws ZookeeperException, ZookeeperNotFoundException {
		
		return getChildren(path, null);
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
		
		if(! serviceState.isInRunningState()) {
			throw new ZookeeperException("Zookeeper is not connected");
		}

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
		final String detailsPath = getDetailsPath();
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
	public String getInstancesPath() {
		return getClusterPath() + "/" + ZookeeperNodeNames.NAME_SYSTEMS;
	}

	/**
	 * Get the path for the systems
	 * 
	 * @param clustername
	 * @return
	 */
	public String getTablesPath() {
		return getClusterPath() + "/" + ZookeeperNodeNames.NAME_TABLES;
	}

	
	/**
	 * Get the path of the zookeeper nodes
	 */
	public String getActiveInstancesPath() {
		return getInstancesPath() + "/active";
	}

	/**
	 * Get the details path
	 * @return
	 */
	public String getDetailsPath() {
		return getInstancesPath() + "/details";
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
					try {
						logger.debug("Path '{}' not found, creating", partialPath);
						zookeeper.create(partialPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} catch(KeeperException e) {
						if(e.code() == Code.NODEEXISTS) {
							// Ignore exception if node was created already
						} else {
							throw e;
						}
					}
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
	 * Reat the given path and return a string - simple version
	 * @param pathName
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public String readPathAndReturnString(final String pathName) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		return readPathAndReturnString(pathName, null);
	}
	
	/**
	 * Read the given path and returns a string result
	 * 
	 * @param pathName
	 * @param watcher
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public String readPathAndReturnString(final String pathName, final Watcher watcher) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final byte[] bytes = readPathAndReturnBytes(pathName, watcher);
		return new String(bytes);
	}

	/**
	 * Read the given path and returns a byte array result
	 * 
	 * @param pathName
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public byte[] readPathAndReturnBytes(final String pathName, final Watcher watcher)
			throws ZookeeperException, ZookeeperNotFoundException {

		try {
			if (zookeeper.exists(pathName, false) == null) {
					throw new ZookeeperNotFoundException("The path does not exist: " + pathName);
			}

			return zookeeper.getData(pathName, watcher, null);
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
	 * Get the service state
	 * @return
	 */
	public ServiceState getServiceState() {
		return serviceState;
	}

	@Override
	public boolean acquire() {
		
		if(! serviceState.isInRunningState()) {
			return false;
		}
		
		assert (! usage.isTerminated()) : "Usage counter is terminated";

		usage.register();
		return true;
	}

	@Override
	public void release() {
		assert (usage.getUnarrivedParties() > 0) : "Usage counter is: " + usage.getUnarrivedParties();
		assert (! usage.isTerminated()) : "Usage counter is terminated";
		
		usage.arriveAndDeregister();
	}

}
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
package org.bboxdb.distribution.membership;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.bboxdb.commons.Retryer;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.SystemInfo;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

public class ZookeeperBBoxDBInstanceAdapter implements Watcher {

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;
	
	/**
	 * The path helper
	 */
	private final ZookeeperInstancePathHelper pathHelper;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperBBoxDBInstanceAdapter.class);

	public ZookeeperBBoxDBInstanceAdapter(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = Objects.requireNonNull(zookeeperClient);
		this.pathHelper = new ZookeeperInstancePathHelper(zookeeperClient);
	}

	/**
	 * Register a watch on membership changes. A watch is a one-time operation,
	 * the watch is reregistered on each method call.
	 */
	public boolean readMembershipAndRegisterWatch() {

		try {
			final BBoxDBInstanceManager distributedInstanceManager 
				= BBoxDBInstanceManager.getInstance();

			// Reregister watch on membership
			final String activeInstancesPath = zookeeperClient.getActiveInstancesPath();
			zookeeperClient.getChildren(activeInstancesPath, this);
			
			// Read version data
			final String detailsPath = zookeeperClient.getDetailsPath();
			final List<String> instances = zookeeperClient.getChildren(detailsPath, this);
			
			final Set<BBoxDBInstance> instanceSet = new HashSet<>();
			
			for (final String instanceName : instances) {
				final BBoxDBInstance distributedInstance = readInstance(instanceName);
				instanceSet.add(distributedInstance);
			}

			distributedInstanceManager.updateInstanceList(instanceSet);
		} catch (ZookeeperNotFoundException | ZookeeperException e) {
			logger.warn("Unable to read membership and create a watch", e);
			return false;
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
 
		return true;
	}

	/**
	 * Read the given instance
	 * @param instanceName
	 * @return
	 * @throws ZookeeperNotFoundException
	 * @throws ZookeeperException 
	 */
	private BBoxDBInstance readInstance(final String instanceName) 
			throws ZookeeperException, InterruptedException {
		
		final BBoxDBInstance instance = new BBoxDBInstance(instanceName);
		
		// After the instance is registered, it can take some time until the
		// complete ressource data is available in zookeeper			
		final Retryer<Boolean> retryer = new Retryer<>(5, 200, TimeUnit.MILLISECONDS, () -> {
			
			// Version
			final String instanceVersion = getVersionForInstance(instance);
			instance.setVersion(instanceVersion);
			
			// State
			final BBoxDBInstanceState state = getStateForInstance(instanceName);
			instance.setState(state);
			
			// CPU cores
			final int cpuCores = getCpuCoresForInstnace(instance);
			instance.setCpuCores(cpuCores);
			
			// Memory
			final long memory = getMemoryForInstance(instance);
			instance.setMemory(memory);
			
			// Diskspace
			readDiskSpaceForInstance(instance);
			
			return true;
		});
	
		final boolean result = retryer.execute();
		
		if(! result) {
			throw new ZookeeperException("Unable to read instance " + instanceName, 
					retryer.getLastException());
		}
		
		return instance;
	}

	/**
	 * Read the state for the given instance
	 * 
	 * @param member
	 * @return
	 * @throws ZookeeperNotFoundException
	 */
	private BBoxDBInstanceState getStateForInstance(final String member) {
		final String nodesPath = zookeeperClient.getActiveInstancesPath();
		final String statePath = nodesPath + "/" + member;

		try {
			final String state = zookeeperClient.readPathAndReturnString(statePath, this);
			if (BBoxDBInstanceState.OUTDATED.getZookeeperValue().equals(state)) {
				return BBoxDBInstanceState.OUTDATED;
			} else if (BBoxDBInstanceState.READY.getZookeeperValue().equals(state)) {
				return BBoxDBInstanceState.READY;
			}
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			// Ignore exception, instance state is unknown
		}

		return BBoxDBInstanceState.FAILED;
	}

	/**
	 * Read the version for the given instance
	 * 
	 * @param instance
	 * @return
	 * @throws ZookeeperNotFoundException
	 */
	private String getVersionForInstance(final BBoxDBInstance instance) throws ZookeeperNotFoundException {
		final String versionPath = pathHelper.getInstancesVersionPath(instance);

		try {
			return zookeeperClient.readPathAndReturnString(versionPath, null);
		} catch (ZookeeperException e) {
			logger.error("Unable to read version for: {}", versionPath);
		}

		return BBoxDBInstance.UNKOWN_PROPERTY;
	}
	
	/**
	 * Get the amount of CPU cores fot the instance
	 * @param instance
	 * @return
	 * @throws ZookeeperNotFoundException
	 */
	private int getCpuCoresForInstnace(final BBoxDBInstance instance) throws ZookeeperNotFoundException {
		final String versionPath = pathHelper.getInstancesCpuCorePath(instance);

		String versionString = null;
		
		try {
			versionString = zookeeperClient.readPathAndReturnString(versionPath);
			return Integer.parseInt(versionString);
		} catch (ZookeeperException e) {
			logger.error("Unable to read cpu cores for: {}", versionPath);
		} catch(NumberFormatException e) {
			logger.error("Unable to parse {} as CPU cores", versionString);
		}

		return -1;
	}
	
	/**
	 * Get the total memory for the given instance
	 * @param instance
	 * @return
	 * @throws ZookeeperNotFoundException
	 */
	private long getMemoryForInstance(final BBoxDBInstance instance) throws ZookeeperNotFoundException {
		final String memoryPath = pathHelper.getInstancesMemoryPath(instance);

		String memoryString = null;

		try {
			memoryString = zookeeperClient.readPathAndReturnString(memoryPath);
			return Long.parseLong(memoryString);
		} catch (ZookeeperException e) {
			logger.error("Unable to read memory for: {}", memoryPath);
		} catch(NumberFormatException e) {
			logger.error("Unable to parse {} as memory", memoryString);
		}

		return -1;
	}
	
	/**
	 * Read the free and the total diskspace for the given instance
	 * @param instance
	 * @throws ZookeeperNotFoundException
	 * @throws ZookeeperException 
	 */
	private void readDiskSpaceForInstance(final BBoxDBInstance instance) 
			throws ZookeeperNotFoundException, ZookeeperException {
		
		final String diskspacePath = pathHelper.getInstancesDiskspacePath(instance);
		
		final List<String> diskspaceChilds = zookeeperClient.getChildren(diskspacePath);
		
		for(final String path : diskspaceChilds) {
			final String unquotedPath = ZookeeperInstancePathHelper.unquotePath(path);
			final String totalDiskspacePath = pathHelper.getInstancesDiskspaceTotalPath(instance, unquotedPath);
			final String freeDiskspacePath = pathHelper.getInstancesDiskspaceFreePath(instance, unquotedPath);
			
			final String totalDiskspaceString = zookeeperClient.readPathAndReturnString(totalDiskspacePath);
			final String freeDiskspaceString = zookeeperClient.readPathAndReturnString(freeDiskspacePath);
			
			final Long totalDiskspace = Longs.tryParse(totalDiskspaceString);
			final Long freeDiskspace = Longs.tryParse(freeDiskspaceString);
							
			if(totalDiskspace == null) {
				logger.error("Unable to parse {} as total diskspace", totalDiskspaceString);
			} else {
				instance.addTotalSpace(unquotedPath, totalDiskspace);
			}
			
			if(freeDiskspace == null) {
				logger.error("Unable to parse {} as free diskspace", freeDiskspaceString);
			} else {
				instance.addFreeSpace(unquotedPath, freeDiskspace);
			}
		}
	}
	

	/**
	 * Zookeeper watched event
	 */
	@Override
	public void process(final WatchedEvent watchedEvent) {

		final boolean aquired = zookeeperClient.acquire();

		try {
			logger.debug("Got zookeeper event: {} " + watchedEvent);
			
			if(aquired) {
				processZookeeperEvent(watchedEvent);
			} else {
				logger.info("Ignoring zookeeper event, unable to aquire zookeeper");
			}
			
		} catch (Throwable e) {
			logger.error("Got uncought exception while processing event", e);
		} finally {
			if(aquired) {
				zookeeperClient.release();
			}
		}

	}

	/**
	 * Process zooekeeper events
	 * 
	 * @param watchedEvent
	 */
	private synchronized void processZookeeperEvent(final WatchedEvent watchedEvent) {
		// Ignore null parameter
		if (watchedEvent == null) {
			logger.warn("process called with an null argument");
			return;
		}

		final ServiceState serviceState = zookeeperClient.getServiceState();
		
		// Shutdown is pending, stop event processing
		if (! serviceState.isInRunningState()) {
			logger.debug("Ignoring event {}, because service state is {}", watchedEvent, serviceState);
			return;
		}

		// Ignore type=none event
		if (watchedEvent.getType() == EventType.None) {
			return;
		}

		// Process events
		if (watchedEvent.getPath() != null) {
			readMembershipAndRegisterWatch();	
		} else {
			logger.warn("Got unknown zookeeper event: {}", watchedEvent);
		}
	}

	/**
	 * Get the zookeeper client
	 * @return
	 */
	public ZookeeperClient getZookeeperClient() {
		return zookeeperClient;
	}
	
	/**
	 * Update the hardware info
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	public void updateNodeInfo(final BBoxDBInstance instance) throws ZookeeperException {
		
		// Version 
		final String versionPath = pathHelper.getInstancesVersionPath(instance);
		zookeeperClient.replacePersistentNode(versionPath, Const.VERSION.getBytes());
		
		// CPUs
		final int cpuCores = SystemInfo.getCPUCores();
		final String cpuCoresPath = pathHelper.getInstancesCpuCorePath(instance);
		zookeeperClient.replacePersistentNode(cpuCoresPath, Integer.toString(cpuCores).getBytes());

		// Memory
		final long memory = SystemInfo.getAvailableMemory();
		final String memoryPath = pathHelper.getInstancesMemoryPath(instance);
		zookeeperClient.replacePersistentNode(memoryPath, Long.toString(memory).getBytes());

		// Diskspace
		final BBoxDBConfiguration bboxDBConfiguration = BBoxDBConfigurationManager.getConfiguration();
		final List<String> directories = bboxDBConfiguration.getStorageDirectories();
		for(final String directory : directories) {
			final File path = new File(directory);
			
			// Free
			final long freeDiskspace = SystemInfo.getFreeDiskspace(path);
			final String freeDiskspacePath = pathHelper.getInstancesDiskspaceFreePath(instance, directory);
			zookeeperClient.replacePersistentNode(freeDiskspacePath, Long.toString(freeDiskspace).getBytes());

			// Total
			final long totalDiskspace = SystemInfo.getTotalDiskspace(path);
			final String totalDiskspacePath = pathHelper.getInstancesDiskspaceTotalPath(instance, directory);
			zookeeperClient.replacePersistentNode(totalDiskspacePath, Long.toString(totalDiskspace).getBytes());
		}
	}

	/**
	 * Update the instance data
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	public void updateStateData(final BBoxDBInstance instance) throws ZookeeperException {
		updateStateData(instance, instance.getState());
	}
	
	/**
	 * Update the instance data
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	public void updateStateData(final BBoxDBInstance instance, final BBoxDBInstanceState newState) 
			throws ZookeeperException {
		
		final String statePath = zookeeperClient.getActiveInstancesPath() + "/" + instance.getStringValue();
		
		logger.info("Update instance state on: {}", statePath);
		zookeeperClient.replaceEphemeralNode(statePath, newState.getZookeeperValue().getBytes());
	}
}

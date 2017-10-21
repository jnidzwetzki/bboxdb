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

import java.io.File;
import java.util.List;
import java.util.function.Consumer;

import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.Const;
import org.bboxdb.util.SystemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstanceRegisterer implements Consumer<ZookeeperClient> {
	
	/**
	 * The name of the instance
	 */
	protected final DistributedInstance instance;

	public InstanceRegisterer() {
		instance = ZookeeperClientFactory.getLocalInstanceName();
	}
	
	public InstanceRegisterer(final DistributedInstance instance) {
		this.instance = instance;
	}
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);
	
	@Override
	public void accept(final ZookeeperClient zookeeperClient) {
		
		if (instance == null) {
			logger.error("Unable to determine local instance name");
			return;
		}

		try {
			updateNodeInfo(zookeeperClient);
			updateStateData(zookeeperClient);
		} catch (ZookeeperException e) {
			logger.error("Exception while registering instance", e);
		}
	}

	/**
	 * Update the instance data
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	protected void updateStateData(final ZookeeperClient zookeeperClient) throws ZookeeperException {
		
		final String statePath = zookeeperClient.getActiveInstancesPath() + "/" + instance.getStringValue();
		
		logger.info("Register instance on: {}", statePath);
		zookeeperClient.replaceEphemeralNode(statePath, instance.getState().getZookeeperValue().getBytes());
	}
	
	/**
	 * Update the hardware info
	 * @param zookeeperClient
	 * @throws ZookeeperException 
	 */
	protected void updateNodeInfo(final ZookeeperClient zookeeperClient) throws ZookeeperException {
		
		// Version 
		final String versionPath = zookeeperClient.getInstancesVersionPath(instance);
		zookeeperClient.replacePersistentNode(versionPath, Const.VERSION.getBytes());
		
		// CPUs
		final int cpuCores = SystemInfo.getCPUCores();
		final String cpuCoresPath = zookeeperClient.getInstancesCpuCorePath(instance);
		zookeeperClient.replacePersistentNode(cpuCoresPath, Integer.toString(cpuCores).getBytes());

		// Memory
		final long memory = SystemInfo.getAvailableMemory();
		final String memoryPath = zookeeperClient.getInstancesMemoryPath(instance);
		zookeeperClient.replacePersistentNode(memoryPath, Long.toString(memory).getBytes());

		// Diskspace
		final BBoxDBConfiguration bboxDBConfiguration = BBoxDBConfigurationManager.getConfiguration();
		final List<String> directories = bboxDBConfiguration.getStorageDirectories();
		for(final String directory : directories) {
			final File path = new File(directory);
			
			// Free
			final long freeDiskspace = SystemInfo.getFreeDiskspace(path);
			final String freeDiskspacePath = zookeeperClient.getInstancesDiskspaceFreePath(instance, directory);
			zookeeperClient.replacePersistentNode(freeDiskspacePath, Long.toString(freeDiskspace).getBytes());

			// Total
			final long totalDiskspace = SystemInfo.getTotalDiskspace(path);
			final String totalDiskspacePath = zookeeperClient.getInstancesDiskspaceTotalPath(instance, directory);
			zookeeperClient.replacePersistentNode(totalDiskspacePath, Long.toString(totalDiskspace).getBytes());
		}
	}
}



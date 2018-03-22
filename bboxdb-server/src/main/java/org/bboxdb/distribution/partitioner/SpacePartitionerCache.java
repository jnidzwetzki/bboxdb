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
package org.bboxdb.distribution.partitioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.NodeMutationHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpacePartitionerCache implements Watcher {
	
	/**
	 * Mapping between the string group and the group object
	 */
	private final Map<String, SpacePartitioner> spacePartitioner;
	
	/**
	 * The versions
	 */
	private final Map<String, Long> partitionerVersions;
	
	/**
	 * The region mapper
	 */
	private final Map<String, DistributionRegionIdMapper> distributionRegionIdMapper;
	
	/**
	 * The callbacks
	 */
	private final Map<String, Set<DistributionRegionCallback>> callbacks;
	
	/**
	 * The zookeeper adapter
	 */
	private final ZookeeperClient zookeeperClient;

	/**
	 * The instance
	 */
	private static SpacePartitionerCache instance;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SpacePartitionerCache.class);

	private SpacePartitionerCache() {
		this.spacePartitioner = new HashMap<>();
		this.distributionRegionIdMapper = new HashMap<>();
		this.partitionerVersions = new HashMap<>();
		this.callbacks = new HashMap<>();
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
	}
	
	public synchronized static SpacePartitionerCache getInstance() {
		 if(instance == null) {
			 instance = new SpacePartitionerCache();
		 }
		 
		 return instance;
	}
	
	/**
	 * Get the distribution region for the given group name
	 * @param groupName
	 * @return
	 * @throws BBoxDBException 
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public synchronized SpacePartitioner getSpacePartitionerForGroupName(final String groupName) 
			throws BBoxDBException {
		
		try {
			if(! spacePartitioner.containsKey(groupName)) {		
				final String path = zookeeperClient
						.getDistributionGroupAdapter().getDistributionGroupPath(groupName);
				
				final long version = NodeMutationHelper
						.getNodeMutationVersion(zookeeperClient, path, this);
				
				// Create callback list
				if(! callbacks.containsKey(groupName)) {
					final Set<DistributionRegionCallback> callback = new CopyOnWriteArraySet<>();
					callbacks.put(groupName, callback);
				}
				
				// Create region id mapper
				if(! distributionRegionIdMapper.containsKey(groupName)) {
					final DistributionRegionIdMapper mapper = new DistributionRegionIdMapper(groupName);
					distributionRegionIdMapper.put(groupName, mapper);
				}
				
				final SpacePartitioner adapter = zookeeperClient
						.getDistributionGroupAdapter().getSpaceparitioner(groupName, 
						callbacks.get(groupName), distributionRegionIdMapper.get(groupName));
				
				partitionerVersions.put(groupName, version);
				spacePartitioner.put(groupName, adapter);
			}
			
			return spacePartitioner.get(groupName);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Get all known distribution groups
	 * @return
	 */
	public synchronized Set<String> getAllKnownDistributionGroups() {
		return new HashSet<>(spacePartitioner.keySet());
	}
	
	/**
	 * Process changed on the registered distribution regions
	 */
	@Override
	public void process(final WatchedEvent event) {
		// Ignore events like connected and disconnected
		if(event == null || event.getPath() == null) {
			return;
		}
		
		final String path = event.getPath();

		// Amount of distribution groups have changed
		if(path.endsWith(ZookeeperNodeNames.NAME_NODE_VERSION)) {
			logger.debug("===> Got event {}", event);
			testGroupRecreatedNE();
		} else {
			logger.debug("===> Ignoring event for path: {}" , path);
		}
	}
	
	/**
	 * Rescan for newly created distribution groups
	 */
	private void testGroupRecreatedNE() {
		try {
			testGroupRecreated();
		} catch (Throwable e) {
			logger.error("Got zookeeper exception", e);
		}
	}
	
	/**
	 * Refresh the whole tree
	 * @throws ZookeeperException 
	 */
	private void testGroupRecreated() throws ZookeeperException {
		
		// Private copy to allow modifications
		final Set<String> knownPartitioner = new HashSet<>(spacePartitioner.keySet());
		
		for(final String groupname : knownPartitioner) {

			try {
				final String path = zookeeperClient
						.getDistributionGroupAdapter().getDistributionGroupPath(groupname);
				
				final long zookeeperVersion 
					= NodeMutationHelper.getNodeMutationVersion(zookeeperClient, path, this);
				
				final long memoryVersion = partitionerVersions.getOrDefault(groupname, 0l);
				
				if(memoryVersion < zookeeperVersion) {
					logger.info("Our space partitioner version is {}, zookeeper version is {}", 
							memoryVersion, zookeeperVersion);
					
					resetSpacePartitioner(groupname);
					getSpacePartitionerForGroupName(groupname);
				} 
			} catch (ZookeeperNotFoundException e) {
				logger.info("Version for {} not found, deleting in memory version", groupname);
				resetSpacePartitioner(groupname);
			} catch (BBoxDBException | ZookeeperException e) {
				logger.error("Got exception while reading dgroup", e);
			}
		}			
	}

	/**
	 * Reset the space partitioner
	 * 
	 * @param groupname
	 */
	private void resetSpacePartitioner(final String groupname) {
		
		final SpacePartitioner deletedSpacePartitioner = spacePartitioner.remove(groupname);
		
		if(deletedSpacePartitioner != null) {
			deletedSpacePartitioner.shutdown();
		}
		
		partitionerVersions.remove(groupname);
		
		TupleStoreConfigurationCache.getInstance().clear();
		DistributionGroupConfigurationCache.getInstance().clear();
		distributionRegionIdMapper.get(groupname).clear();
	}


	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("Unable to clone a singleton");
	}
}

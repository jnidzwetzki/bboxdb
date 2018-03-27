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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.Watcher;
import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.SpacePartitionerContext;
import org.bboxdb.distribution.partitioner.SpacePartitionerFactory;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionGroupAdapter {

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionGroupAdapter.class);

	public DistributionGroupAdapter(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
	}
	
	/**
	 * Get the next table id for a given distribution group
	 * @return
	 * @throws ZookeeperException 
	 */
	public int getNextTableIdForDistributionGroup(final String distributionGroup) throws ZookeeperException {
		
		final String distributionGroupIdQueuePath = getDistributionGroupIdQueuePath(distributionGroup);
		
		zookeeperClient.createDirectoryStructureRecursive(distributionGroupIdQueuePath);
	
		final String nodePath = distributionGroupIdQueuePath + "/" 
				+ ZookeeperNodeNames.SEQUENCE_QUEUE_PREFIX;
		
		final String nodename = zookeeperClient.createPersistentSequencialNode(
				nodePath, "".getBytes());
		
		// Delete the created node
		logger.debug("Got new table id; deleting node: {}", nodename);
		
		zookeeperClient.deleteNodesRecursive(nodename);
		
		// id-0000000063
		// Element 0: id-
		// Element 1: The number of the node
		final String[] splittedName = nodename.split(ZookeeperNodeNames.SEQUENCE_QUEUE_PREFIX);
		
		try {
			return Integer.parseInt(splittedName[1]);
		} catch(NumberFormatException e) {
			logger.warn("Unable to parse number: " + splittedName[1], e);
			throw new ZookeeperException(e);
		}
	}
	
	/**
	 * Get the space partitioner of a distribution group
	 * @param mapper 
	 * @param callback 
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public SpacePartitioner getSpaceparitioner(final String distributionGroup, 
			final Set<DistributionRegionCallback> callback, final DistributionRegionIdMapper mapper) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final String path = getDistributionGroupPath(distributionGroup);

		if(! zookeeperClient.exists(path)) {
			final String exceptionMessage = MessageFormat.format("Unable to read {0}. Path {1} does not exist", distributionGroup, path);
			throw new ZookeeperException(exceptionMessage);
		}
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(distributionGroup);
				
		final SpacePartitionerContext spacePartitionerContext = new SpacePartitionerContext(
				config.getSpacePartitionerConfig(), 
				distributionGroup, 
				zookeeperClient, 
				callback, 
				mapper);
		
		return SpacePartitionerFactory.getSpacePartitionerForDistributionGroup(spacePartitionerContext);
	}
	

	/**
	 * Create a new distribution group
	 * @param distributionGroup
	 * @param replicationFactor
	 * @throws ZookeeperException 
	 * @throws BBoxDBException 
	 */
	public void createDistributionGroup(final String distributionGroup, 
			final DistributionGroupConfiguration configuration) 
					throws ZookeeperException, BBoxDBException {
		
		final String path = getDistributionGroupPath(distributionGroup);
		
		zookeeperClient.createDirectoryStructureRecursive(path);
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_DIMENSIONS, 
				Integer.toString(configuration.getDimensions()).getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_REPLICATION, 
				Short.toString(configuration.getReplicationFactor()).getBytes());
		
		setRegionSizeForDistributionGroup(distributionGroup, configuration.getMaximumRegionSize(), 
				configuration.getMinimumRegionSize());
		
		// Placement
		final String placementPath = path + "/" + ZookeeperNodeNames.NAME_PLACEMENT_STRATEGY;
		zookeeperClient.replacePersistentNode(placementPath, configuration.getPlacementStrategy().getBytes());
		final String placementConfigPath = path + "/" + ZookeeperNodeNames.NAME_PLACEMENT_CONFIG;
		zookeeperClient.replacePersistentNode(placementConfigPath, configuration.getPlacementStrategyConfig().getBytes());
		
		// Space partitioner
		final String spacePartitionerPath = path + "/" + ZookeeperNodeNames.NAME_SPACEPARTITIONER;
		zookeeperClient.replacePersistentNode(spacePartitionerPath, configuration.getSpacePartitioner().getBytes());
		final String spacePartitionerConfigPath = path + "/" + ZookeeperNodeNames.NAME_SPACEPARTITIONER_CONFIG;
		zookeeperClient.replacePersistentNode(spacePartitionerConfigPath, configuration.getSpacePartitionerConfig().getBytes());
		
		final TupleStoreAdapter tupleStoreAdapter = new TupleStoreAdapter(zookeeperClient);
		tupleStoreAdapter.createTupleStoreConfigNode(distributionGroup);
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, path);
		
		final SpacePartitioner spacePartitioner 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroup);
		
		spacePartitioner.createRootNode(configuration);
	}

	/**
	 * Get the path for the distribution group id queue
	 * @param distributionGroup
	 * @return
	 */
	public String getDistributionGroupIdQueuePath(final String distributionGroup) {
		 return getDistributionGroupPath(distributionGroup) 
				 + "/" + ZookeeperNodeNames.NAME_PREFIXQUEUE;
	}
	
	/**
	 * Get the path for the distribution group
	 * @param distributionGroup
	 * @return
	 */
	public String getDistributionGroupPath(final String distributionGroup) {
		return zookeeperClient.getClusterPath() + "/" + distributionGroup;
	}
	
	/**
	 * Get the path of the root element of th group
	 * @param distributionGroup
	 * @return
	 */
	public String getDistributionGroupRootElementPath(final String distributionGroup) {
		return getDistributionGroupPath(distributionGroup) + "/" + ZookeeperNodeNames.NAME_ROOT_NODE;
	}
	
	
	/**
	 * Return the path for the cluster
	 * @return
	 */
	public String getClusterPath() {
		return zookeeperClient.getClusterPath();
	}

	/**
	 * Delete an existing distribution group
	 * @param distributionGroup
	 * @throws ZookeeperException 
	 */
	public void deleteDistributionGroup(final String distributionGroup) throws ZookeeperException {
		final String path = getDistributionGroupPath(distributionGroup);			
		zookeeperClient.deleteNodesRecursive(path);
	}
	
	/**
	 * Does the distribution group exists?
	 * @param distributionGroup
	 * @return 
	 * @throws ZookeeperException
	 */
	public boolean isDistributionGroupRegistered(final String distributionGroup) throws ZookeeperException {
		final String path = getDistributionGroupPath(distributionGroup);

		return NodeMutationHelper.isNodeCompletelyCreated(zookeeperClient, path);
	}
	
	/**
	 * List all existing distribution groups
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public List<String> getDistributionGroups() 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		return getDistributionGroups(null);
	}

	/**
	 * List all existing distribution groups
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public List<String> getDistributionGroups(final Watcher watcher) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final List<String> groups = new ArrayList<>();
		final String clusterPath = zookeeperClient.getClusterPath();
		final List<String> nodes = zookeeperClient.getChildren(clusterPath, watcher);
		
		for(final String groupName : nodes) {
			
			// Ignore systems
			if(ZookeeperNodeNames.NAME_SYSTEMS.equals(groupName)) {
				continue;
			}
			
			if(DistributionGroupHelper.validateDistributionGroupName(groupName)) {
				groups.add(groupName);
			} else {
				logger.error("Got invalid distribution group name from zookeeper: {}", groupName);
			}
		}
		
		return groups;
	}
	
	/**
	 * Get the name prefix for a given path
	 * @param path
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public int getRegionIdForPath(final String path) throws ZookeeperException, ZookeeperNotFoundException {
		
		final String namePrefixPath = path + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX;
		String namePrefix = null;
		
		try {
			namePrefix = zookeeperClient.readPathAndReturnString(namePrefixPath);
			return Integer.parseInt(namePrefix);
		} catch (NumberFormatException e) {
			throw new ZookeeperException("Unable to parse name prefix '" + namePrefix + "' for " + namePrefixPath);
		}		
	}
	
	/**
	 * Get the distribution group configuration
	 * @param distributionGroup
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws InputParseException
	 */
	public DistributionGroupConfiguration getDistributionGroupConfiguration(
			final String distributionGroup) throws ZookeeperException, ZookeeperNotFoundException, 
			InputParseException {
		
		final String path = getDistributionGroupPath(distributionGroup);
		final String placementConfigPath = path + "/" + ZookeeperNodeNames.NAME_PLACEMENT_CONFIG;
		final String placementConfig = zookeeperClient.readPathAndReturnString(placementConfigPath);
	
		final String placementPath = path + "/" + ZookeeperNodeNames.NAME_PLACEMENT_STRATEGY;
		final String placementStrategy = zookeeperClient.readPathAndReturnString(placementPath);

		final String spacePartitionerConfigPath = path + "/" + ZookeeperNodeNames.NAME_SPACEPARTITIONER_CONFIG;
		final String spacePartitionerConfig = zookeeperClient.readPathAndReturnString(spacePartitionerConfigPath);
			
		final String spacePartitionerPath = path + "/" + ZookeeperNodeNames.NAME_SPACEPARTITIONER;
		final String spacePartitoner = zookeeperClient.readPathAndReturnString(spacePartitionerPath);
		
		final String replicationFactorPath = path + "/" + ZookeeperNodeNames.NAME_REPLICATION;
		final String replicationFactorString = zookeeperClient.getData(replicationFactorPath);
		final short replicationFactor = (short) MathUtil.tryParseInt(replicationFactorString, 
				() -> "Unable to parse: " + replicationFactorString);
		
		final String dimensionsPath = path + "/" + ZookeeperNodeNames.NAME_DIMENSIONS;
		final String dimensionsString = zookeeperClient.getData(dimensionsPath);
		final int dimensions = MathUtil.tryParseInt(dimensionsString, () -> "Unable to parse: " + dimensionsString);
		
		final String regionMinSizePath = path + "/" + ZookeeperNodeNames.NAME_MIN_REGION_SIZE;
		final String sizeStrinMin = zookeeperClient.readPathAndReturnString(regionMinSizePath);
		final int minRegionSize =  MathUtil.tryParseInt(sizeStrinMin, () -> "Unable to parse: " + sizeStrinMin);
		
		final String regionMaxSizePath = path + "/" + ZookeeperNodeNames.NAME_MAX_REGION_SIZE;
		final String sizeStringMax = zookeeperClient.readPathAndReturnString(regionMaxSizePath);
		final int maxRegionSize =  MathUtil.tryParseInt(sizeStringMax, () -> "Unable to parse: " + sizeStringMax);

		final DistributionGroupConfiguration configuration = new DistributionGroupConfiguration();
		configuration.setPlacementStrategyConfig(placementConfig);
		configuration.setPlacementStrategy(placementStrategy);
		configuration.setSpacePartitionerConfig(spacePartitionerConfig);
		configuration.setSpacePartitioner(spacePartitoner);
		configuration.setReplicationFactor(replicationFactor);
		configuration.setMaximumRegionSize(maxRegionSize);
		configuration.setMinimumRegionSize(minRegionSize);
		configuration.setDimensions(dimensions);
		
		return configuration;
	}
	
	/**
	 * Set the region size
	 * @param maxRegionSize
	 * @throws ZookeeperException 
	 */
	public void setRegionSizeForDistributionGroup(final String distributionGroup, 
			final int maxRegionSize, final int minRegionSize) 
			throws ZookeeperException {
		
		final String path = getDistributionGroupPath(distributionGroup);
		
		// Max region size
		final String maxRegionSizePath = path + "/" + ZookeeperNodeNames.NAME_MAX_REGION_SIZE;
		zookeeperClient.replacePersistentNode(maxRegionSizePath, Integer.toString(maxRegionSize).getBytes());
		
		// Min region size
		final String minRegionSizePath = path + "/" + ZookeeperNodeNames.NAME_MIN_REGION_SIZE;
		zookeeperClient.replacePersistentNode(minRegionSizePath, Integer.toString(minRegionSize).getBytes());
	}


}

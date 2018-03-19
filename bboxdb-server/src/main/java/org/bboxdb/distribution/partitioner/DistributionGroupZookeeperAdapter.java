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

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.zookeeper.Watcher;
import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionGroupZookeeperAdapter {

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionGroupZookeeperAdapter.class);

	public DistributionGroupZookeeperAdapter(final ZookeeperClient zookeeperClient) {
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
	 * Get the split position for a given path
	 * @param path
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 */
	public BoundingBox getBoundingBoxForPath(final String path) throws ZookeeperException, ZookeeperNotFoundException  {
		
		final String splitPathName = path + "/" + ZookeeperNodeNames.NAME_BOUNDINGBOX;
		String boundingBoxString = null;
		
		try {			
			boundingBoxString = zookeeperClient.readPathAndReturnString(splitPathName);
			return new BoundingBox(boundingBoxString);
		} catch (NumberFormatException e) {
			throw new ZookeeperException("Unable to parse bounding box '" + boundingBoxString + "' for " + splitPathName);
		}		
	}
	
	/**
	 * Set the split position for the given path
	 * @param path
	 * @param position
	 * @throws ZookeeperException 
	 */
	public void setBoundingBoxForPath(final String path, final BoundingBox boundingBox) 
			throws ZookeeperException {
		
		final String boundingBoxString = boundingBox.toCompactString();
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_BOUNDINGBOX, 
				boundingBoxString.getBytes());		
	}
	
	/**
	 * Update the version for the node
	 * @param path
	 * @param position
	 * @throws ZookeeperException 
	 */
	public void markNodeMutationAsComplete(final String path) throws ZookeeperException {
		final ByteBuffer versionBytes = DataEncoderHelper.longToByteBuffer(System.currentTimeMillis());
		
		final String nodePath = path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION;
		
		logger.debug("Mark mutation as complete {}", path);
		
		zookeeperClient.replacePersistentNode(nodePath, versionBytes.array());
	}
	
	/**
	 * Is the node completely created?
	 * @param path
	 * @return
	 * @throws ZookeeperException 
	 */
	public boolean isNodeCompletelyCreated(final String path) throws ZookeeperException {
		return zookeeperClient.exists(path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION);
	}
	
	/**
	 * Get the version of the node mutation
	 * @param path
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public long getNodeMutationVersion(final String path, final Watcher watcher) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		logger.debug("Reading mutation from path {}", path);
		
		final byte[] result = zookeeperClient.readPathAndReturnBytes(
				path + "/" + ZookeeperNodeNames.NAME_NODE_VERSION, watcher);
		
		return DataEncoderHelper.readLongFromByte(result);
	}
	
	/**
	 * Get the state for a given path - version without a watcher
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public DistributionRegionState getStateForDistributionRegion(final String path) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		return getStateForDistributionRegion(path, null);
	}
	
	/**
	 * Get the state for a given path
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public DistributionRegionState getStateForDistributionRegion(final String path, 
			final Watcher callback) throws ZookeeperException, ZookeeperNotFoundException {
		
		final String statePath = path + "/" + ZookeeperNodeNames.NAME_REGION_STATE;
		final String state = zookeeperClient.readPathAndReturnString(statePath, callback);
		return DistributionRegionState.fromString(state);
	}
	
	/**
	 * Set the given region to full (if possible)
	 * @param region
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public boolean setToFull(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		logger.debug("Set state for {} to full", region.getIdentifier());
		
		final String zookeeperPath = getZookeeperPathForDistributionRegionState(region);
				
		final DistributionRegionState oldState = getStateForDistributionRegion(region);
		
		if(oldState != DistributionRegionState.ACTIVE) {
			logger.debug("Old state is not active (old value {})" , oldState);
			return false;
		}
		
		final boolean result = zookeeperClient.testAndReplaceValue(zookeeperPath, 
				DistributionRegionState.ACTIVE.getStringValue(), 
				DistributionRegionState.ACTIVE_FULL.getStringValue());
		
		markNodeMutationAsComplete(zookeeperPath);
		
		return result;
	}
	
	/**
	 * Set the given region to full (if possible)
	 * @param region
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public boolean setToSplitMerging(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		logger.debug("Set state for {} to merging", region.getIdentifier());
		
		final String zookeeperPath = getZookeeperPathForDistributionRegionState(region);
				
		final DistributionRegionState oldState = getStateForDistributionRegion(region);
		
		if(oldState != DistributionRegionState.SPLIT) {
			logger.debug("Old state is not active (old value {})" , oldState);
			return false;
		}
		
		final boolean result = zookeeperClient.testAndReplaceValue(zookeeperPath, 
				DistributionRegionState.SPLIT.getStringValue(), 
				DistributionRegionState.SPLIT_MERGING.getStringValue());
		
		markNodeMutationAsComplete(zookeeperPath);

		return result;
	}
	
	/**
	 * Get the state for a given path - without watcher
	 * @return 
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public DistributionRegionState getStateForDistributionRegion(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException  {
		
		return getStateForDistributionRegion(region, null);
	}

	/**
	 * Get the state for a given path
	 * @return 
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public DistributionRegionState getStateForDistributionRegion(final DistributionRegion region, 
			final Watcher callback) throws ZookeeperException, ZookeeperNotFoundException  {
		
		final String path = getZookeeperPathForDistributionRegion(region);
		return getStateForDistributionRegion(path, callback);
	}
	
	/**
	 * Set the state for a given path
	 * @param path
	 * @param state
	 * @throws ZookeeperException 
	 */
	public void setStateForDistributionGroup(final String path, final DistributionRegionState state) 
			throws ZookeeperException  {
		
		logger.debug("Set state {} for path {}", state, path);
		
		final String statePath = path + "/" + ZookeeperNodeNames.NAME_REGION_STATE;
		zookeeperClient.setData(statePath, state.getStringValue());
		
		markNodeMutationAsComplete(path);
	}
	
	/**
	 * Set the state for a given distribution region
	 * @param region
	 * @param state
	 * @throws ZookeeperException
	 */
	public void setStateForDistributionRegion(final DistributionRegion region, 
			final DistributionRegionState state) throws ZookeeperException  {
		
		final String path = getZookeeperPathForDistributionRegion(region);
		setStateForDistributionGroup(path, state);
		
		markNodeMutationAsComplete(path);
	}
	
	/**
	 * Get the path for the distribution region state
	 * @param region
	 * @return
	 */
	protected String getZookeeperPathForDistributionRegionState(final DistributionRegion region) {
		
		return getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_REGION_STATE;
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
					
		markNodeMutationAsComplete(path);
		
		final SpacePartitioner spacePartitioner 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroup);
		
		spacePartitioner.createRootNode(configuration);
	}
	
	/**
	 * Set the merging supported flag
	 * @param region
	 * @param suppported
	 * @throws ZookeeperException 
	 */
	public void setMergingSupported(final DistributionRegion region, final boolean suppported)
			throws ZookeeperException {
		
		final String pathMerge = getMergePath(region);
		final String value = Boolean.toString(suppported);
		
		zookeeperClient.replacePersistentNode(pathMerge, value.getBytes());
	}

	/**
	 * Get the merge path
	 * @param region
	 * @return
	 */
	private String getMergePath(final DistributionRegion region) {
		final String path = getZookeeperPathForDistributionRegion(region);
		return path + "/" + ZookeeperNodeNames.NAME_MERGING_SUPPORTED;
	}
	
	/**
	 * Is merging of the region supported?
	 * @param region
	 * @return
	 * @throws ZookeeperException
	 */
	public boolean isMergingSupported(final DistributionRegion region) throws BBoxDBException {
		final String pathMerge = getMergePath(region);
		
		try {
			final String value = zookeeperClient.readPathAndReturnString(pathMerge);
			return Boolean.parseBoolean(value);
		} catch (ZookeeperNotFoundException e) {
			// Not configured default: true
			return true;
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	/**
	 * Create a new child
	 * @param childNumber 
	 * @param leftBoundingBox 
	 * @param path
	 * @return 
	 * @throws ZookeeperException
	 */
	public String createNewChild(final String parentPath, final int childNumber, 
			final BoundingBox boundingBox, final String distributionGroupName) throws ZookeeperException {

		final String childPath = parentPath + "/" + ZookeeperNodeNames.NAME_CHILDREN + childNumber;
		logger.info("Creating: {}", childPath);
		
		if(zookeeperClient.exists(childPath)) {
			throw new ZookeeperException("Child already exists: " + childPath);
		}

		zookeeperClient.createPersistentNode(childPath, "".getBytes());
		
		final int namePrefix = getNextTableIdForDistributionGroup(distributionGroupName);
		
		zookeeperClient.createPersistentNode(childPath + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
				Integer.toString(namePrefix).getBytes());
		
		logger.info("Set {} to {}", childPath, namePrefix);
		
		zookeeperClient.createPersistentNode(childPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
				"".getBytes());
		
		setBoundingBoxForPath(childPath, boundingBox);
				
		zookeeperClient.createPersistentNode(childPath + "/" + ZookeeperNodeNames.NAME_REGION_STATE, 
				DistributionRegionState.CREATING.getStringValue().getBytes());
			
		markNodeMutationAsComplete(childPath);
		markNodeMutationAsComplete(parentPath);

		return childPath;
	}
	
	/**
	 * Delete the given child
	 * @param region
	 * @throws BBoxDBException 
	 * @throws ZookeeperException 
	 */
	public void deleteChild(final DistributionRegion region) throws ZookeeperException {
		
		assert(region.isLeafRegion()) : "Region is not a leaf region: " + region;
		
		final String zookeeperPath = getZookeeperPathForDistributionRegion(region);
		
		zookeeperClient.deleteNodesRecursive(zookeeperPath);
		
		if(! region.isRootElement()) {
			final String parentPath = getZookeeperPathForDistributionRegion(region.getParent());
			markNodeMutationAsComplete(parentPath);
		}
	}
	
	/**
	 * Get the zookeeper path for a distribution region
	 * @param distributionRegion
	 * @return
	 */
	public String getZookeeperPathForDistributionRegion(final DistributionRegion distributionRegion) {
		
		final StringBuilder sb = new StringBuilder();
		
		DistributionRegion tmpRegion = distributionRegion;
		
		if(tmpRegion != null) {
			while(! tmpRegion.isRootElement()) {
				sb.insert(0, "/" + ZookeeperNodeNames.NAME_CHILDREN + tmpRegion.getChildNumberOfParent());	
				tmpRegion = tmpRegion.getParent();
			}
		}
		
		final String name = distributionRegion.getDistributionGroupName();
		sb.insert(0, getDistributionGroupRootElementPath(name));
		return sb.toString();
	}
	
	/**
	 * Get the node for the given zookeeper path
	 * @param distributionRegion
	 * @param path
	 * @return
	 */
	public DistributionRegion getNodeForPath(final DistributionRegion distributionRegion, 
			final String path) {
		
		final String name = distributionRegion.getDistributionGroupName();
		final String distributionGroupPath = getDistributionGroupRootElementPath(name);
		
		if(! path.startsWith(distributionGroupPath)) {
			throw new IllegalArgumentException("Path " + path + " does not start with " + distributionGroupPath);
		}
		
		final StringBuilder sb = new StringBuilder(path);
		sb.delete(0, distributionGroupPath.length());
		
		DistributionRegion resultElement = distributionRegion;
		final StringTokenizer tokenizer = new StringTokenizer(sb.toString(), "/");
		
		while(tokenizer.hasMoreTokens()) {
			
			// Element is removed
			if(resultElement == null) {
				return null;
			}
			
			final String token = tokenizer.nextToken();
			if(! token.startsWith(ZookeeperNodeNames.NAME_CHILDREN)) {
				throw new IllegalArgumentException("Unable to decode " + sb);
			}
			
			final String split[] = token.split("-");
			final int childNumber = Integer.parseInt(split[1]);
			
			if(resultElement.getDirectChildren().size() <= childNumber) {
				return null;
			}
			
			resultElement = resultElement.getDirectChildren().get(childNumber);
		}
		
		return resultElement;
	}
	
	/**
	 * Get the systems for the distribution region
	 * @param region
	 * @param callback 
	 * @return
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public Collection<BBoxDBInstance> getSystemsForDistributionRegion(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
	
		final String path = getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_SYSTEMS;
		
		final List<String> children = zookeeperClient.getChildren(path);
		
		return children.stream()
			.map(c -> new BBoxDBInstance(c))
			.collect(Collectors.toList());
	}
	
	/**
	 * Add a system to a distribution region
	 * @param region
	 * @param system
	 * @throws ZookeeperException 
	 */
	public void addSystemToDistributionRegion(final String regionPath, 
			final BBoxDBInstance system) throws ZookeeperException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to add system with value null");
		}
	
		final String systemsPath = regionPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS;
		final String instancePath = systemsPath + "/" + system.getStringValue();

		logger.debug("Register system under systems node: {}", systemsPath);
		
		zookeeperClient.createPersistentNode(instancePath, "".getBytes());
		
		markNodeMutationAsComplete(regionPath);
	}
	
	/**
	 * Set the checkpoint for the distribution region and system
	 * @param region
	 * @param system
	 * @throws ZookeeperException
	 * @throws InterruptedException 
	 */
	public void setCheckpointForDistributionRegion(final DistributionRegion region, 
			final BBoxDBInstance system, final long checkpoint) 
					throws ZookeeperException, InterruptedException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to add system with value null");
		}
		
		final String path = getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_SYSTEMS + "/" + system.getStringValue();
		
		logger.debug("Set checkpoint for: {} to {}", path, checkpoint);
		
		if(! zookeeperClient.exists(path)) {
			throw new ZookeeperException("Path " + path + " does not exists");
		}
		
		zookeeperClient.setData(path, Long.toString(checkpoint));
	}
	
	/**
	 * Get the checkpoint for the distribution region and system
	 * @param region
	 * @param system
	 * @return 
	 * @throws ZookeeperException
	 */
	public long getCheckpointForDistributionRegion(final DistributionRegion region, 
			final BBoxDBInstance system) throws ZookeeperException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to add system with value null");
		}
		
		try {
			final String path = getZookeeperPathForDistributionRegion(region) 
					+ "/" + ZookeeperNodeNames.NAME_SYSTEMS + "/" + system.getStringValue();
		
			if(! zookeeperClient.exists(path)) {
				throw new ZookeeperException("Path " + path + " does not exists");
			}

			final String checkpointString = zookeeperClient.getData(path);
			
			if("".equals(checkpointString)) {
				return -1;
			}
			
			return Long.parseLong(checkpointString);
		} catch (NumberFormatException e) {
			throw new ZookeeperException(e);
		}
	}
			
	/**
	 * Delete a system to a distribution region
	 * @param region
	 * @param system
	 * @return 
	 * @throws ZookeeperException 
	 */
	public boolean deleteSystemFromDistributionRegion(final DistributionRegion region, 
			final BBoxDBInstance system) throws ZookeeperException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to delete system with value null");
		}

		final String path = getZookeeperPathForDistributionRegion(region) + "/" 
				+ ZookeeperNodeNames.NAME_SYSTEMS + "/" + system.getStringValue();
		
		if(! zookeeperClient.exists(path)) {
			return false;
		}
		
		zookeeperClient.deleteNodesRecursive(path);
	
		return true;
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

		return isNodeCompletelyCreated(path);
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

	/**
	 * Update the region statistics
	 * @param region
	 * @param system
	 * @param size
	 * @param tuple
	 * @return
	 * @throws ZookeeperException
	 */
	public void updateRegionStatistics(final DistributionRegion region, 
			final BBoxDBInstance system, final long size, final long tuple) throws ZookeeperException {
		
		if(system == null) {
			throw new IllegalArgumentException("Unable to add system with value null");
		}
		
		logger.debug("Update region statistics for {} / {}", region.getDistributionGroupName(), system);
	
		final String path = getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_STATISTICS + "/" + system.getStringValue();
		
		zookeeperClient.createDirectoryStructureRecursive(path);
		
		final String sizePath = path + "/" + ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE;
		zookeeperClient.replacePersistentNode(sizePath, Long.toString(size).getBytes());
		
		final String tuplePath = path + "/" + ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES;
		zookeeperClient.replacePersistentNode(tuplePath, Long.toString(tuple).getBytes());
	}
	
	/**
	 * Get the statistics for a given region
	 * @param region
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public Map<BBoxDBInstance, Map<String, Long>> getRegionStatistics(final DistributionRegion region) 
			throws ZookeeperException {
		
		final Map<BBoxDBInstance, Map<String, Long>> result = new HashMap<>();
		
		logger.debug("Get statistics for {}", region.getDistributionGroupName());
				
		final String statisticsPath = getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_STATISTICS;
		
		try {
			final List<String> children = zookeeperClient.getChildren(statisticsPath);
			processStatistics(result, statisticsPath, children);
		} catch (ZookeeperNotFoundException e) {
			// No statistics are found, return empty result
		}		
		
		return result;
	}

	/**
	 * @param result
	 * @param statisticsPath
	 * @param childs
	 * @throws ZookeeperException
	 */
	private void processStatistics(final Map<BBoxDBInstance, Map<String, Long>> result, 
			final String statisticsPath, final List<String> childs) throws ZookeeperException {
		
		for(final String system : childs) {
			final String path = statisticsPath + "/" + system;
		
			final Map<String, Long> systemMap = new HashMap<>();
			
			try {
				final String sizePath = path + "/" + ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE;
				if(zookeeperClient.exists(sizePath)) {
					final String sizeString = zookeeperClient.readPathAndReturnString(sizePath);
					final long size = MathUtil.tryParseLong(sizeString, () -> "Unable to parse " + sizeString);
					systemMap.put(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE, size);
				}
				
				final String tuplePath = path + "/" + ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES;
				if(zookeeperClient.exists(tuplePath)) {
					final String tuplesString = zookeeperClient.readPathAndReturnString(tuplePath);
					final long tuples = MathUtil.tryParseLong(tuplesString, () -> "Unable to parse " + tuplesString);
					systemMap.put(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES, tuples);
				}
				
				result.put(new BBoxDBInstance(system), systemMap);
			} catch (InputParseException | ZookeeperNotFoundException e) {
				logger.error("Unable to read statistics", e);
			}
		}
	}
	
	/**
	 * Delete the statistics for a given region
	 * @param region
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public void deleteRegionStatistics(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
				
		logger.debug("Delete statistics for {}", region.getDistributionGroupName());
				
		final String statisticsPath = getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_STATISTICS;
		
		zookeeperClient.deleteNodesRecursive(statisticsPath);
	}
	
	/**
	 * Allocate the given list of systems to a region
	 * @param region
	 * @param allocationSystems
	 * @throws ZookeeperException
	 */
	public void allocateSystemsToRegion(final String regionPath, final Set<BBoxDBInstance> allocationSystems)
			throws ZookeeperException {
		
		final List<String> systemNames = allocationSystems.stream()
				.map(s -> s.getStringValue())
				.collect(Collectors.toList());
		
		logger.info("Allocating region {} to {}", regionPath, systemNames);
		
		// Resource allocation successfully, write data to zookeeper
		for(final BBoxDBInstance instance : allocationSystems) {
			addSystemToDistributionRegion(regionPath, instance);
		}
	}
	

}

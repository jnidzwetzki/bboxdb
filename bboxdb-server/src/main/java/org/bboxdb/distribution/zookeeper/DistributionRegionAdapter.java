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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.zookeeper.Watcher;
import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionAdapter {

	/**
	 * The zookeeper client
	 */
	private ZookeeperClient zookeeperClient;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionAdapter.class);


	public DistributionRegionAdapter(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
	}
	

	/**
	 * Get the bounding box position for a given path
	 * @param path
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 */
	public Hyperrectangle getBoundingBoxForPath(final String path) throws ZookeeperException, ZookeeperNotFoundException  {
		
		final String splitPathName = path + "/" + ZookeeperNodeNames.NAME_BOUNDINGBOX;
		String boundingBoxString = null;
		
		try {			
			boundingBoxString = zookeeperClient.readPathAndReturnString(splitPathName);
			return new Hyperrectangle(boundingBoxString);
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
	public void setBoundingBoxForPath(final String path, final Hyperrectangle boundingBox) 
			throws ZookeeperException {
		
		final String boundingBoxString = boundingBox.toCompactString();
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_BOUNDINGBOX, 
				boundingBoxString.getBytes());		
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
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, zookeeperPath);
		
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
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, zookeeperPath);

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
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, path);
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
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, path);
	}
	
	/**
	 * Get the path for the distribution region state
	 * @param region
	 * @return
	 */
	private String getZookeeperPathForDistributionRegionState(final DistributionRegion region) {
		
		return getZookeeperPathForDistributionRegion(region) 
				+ "/" + ZookeeperNodeNames.NAME_REGION_STATE;
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
		final String distributionGroupPath 
			= zookeeperClient.getDistributionGroupAdapter().getDistributionGroupRootElementPath(name);
		
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
			
			resultElement = resultElement.getChildNumber(childNumber);			
		}
		
		return resultElement;
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
			NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, parentPath);
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
		final DistributionGroupAdapter distributionGroupAdapter = zookeeperClient.getDistributionGroupAdapter();
		sb.insert(0, distributionGroupAdapter.getDistributionGroupRootElementPath(name));
		return sb.toString();
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
		
		zookeeperClient.replacePersistentNode(instancePath, "".getBytes());
		
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, regionPath);
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
	public String createNewChild(final String parentPath, final long childNumber, 
			final Hyperrectangle boundingBox, final String distributionGroupName) throws ZookeeperException {

		final String childPath = parentPath + "/" + ZookeeperNodeNames.NAME_CHILDREN + childNumber;
		logger.info("Creating: {}", childPath);
		
		if(zookeeperClient.exists(childPath)) {
			throw new ZookeeperException("Child already exists: " + childPath);
		}

		zookeeperClient.createPersistentNode(childPath, "".getBytes());
		
		final DistributionGroupAdapter distributionGroupAdapter 
			= zookeeperClient.getDistributionGroupAdapter();
		
		final int namePrefix 
			= distributionGroupAdapter.getNextTableIdForDistributionGroup(distributionGroupName);
		
		zookeeperClient.createPersistentNode(childPath + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
				Integer.toString(namePrefix).getBytes());
		
		logger.info("Set {} to {}", childPath, namePrefix);
		
		zookeeperClient.createPersistentNode(childPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
				"".getBytes());
		
		setBoundingBoxForPath(childPath, boundingBox);
				
		zookeeperClient.createPersistentNode(childPath + "/" + ZookeeperNodeNames.NAME_REGION_STATE, 
				DistributionRegionState.CREATING.getStringValue().getBytes());
			
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, childPath);
		NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, parentPath);

		return childPath;
	}
	
}


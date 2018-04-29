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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionSyncerHelper;
import org.bboxdb.distribution.zookeeper.NodeMutationHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public abstract class AbstractTreeSpacePartitoner extends AbstractSpacePartitioner {

	/**
	 * The logger
	 */
	final static Logger logger = LoggerFactory.getLogger(AbstractTreeSpacePartitoner.class);

	
	@Override
	public void createRootNode(final DistributionGroupConfiguration configuration) throws BBoxDBException {
		try {
			final String distributionGroup 
				= spacePartitionerContext.getDistributionGroupName();
			
			final String rootPath = 
					distributionGroupZookeeperAdapter.getDistributionGroupRootElementPath(distributionGroup);
			
			zookeeperClient.createDirectoryStructureRecursive(rootPath);
			
			final int nameprefix = distributionGroupZookeeperAdapter
					.getNextTableIdForDistributionGroup(distributionGroup);
						
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
					Integer.toString(nameprefix).getBytes());
			
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
					"".getBytes());
					
			final Hyperrectangle rootBox = getRootBox(configuration);
			distributionRegionZookeeperAdapter.setBoundingBoxForPath(rootPath, rootBox);

			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_REGION_STATE, 
					DistributionRegionState.ACTIVE.getStringValue().getBytes());		
						
			SpacePartitionerHelper.allocateSystemsToRegion(rootPath, distributionGroup,
					new HashSet<>(), zookeeperClient);
			
			NodeMutationHelper.markNodeMutationAsComplete(zookeeperClient, rootPath);
		} catch (ZookeeperException | ResourceAllocationException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		}
	}
	
	/**
	 * Get the root box
	 * @param configuration
	 * @return
	 */
	private Hyperrectangle getRootBox(DistributionGroupConfiguration configuration) {
		
		final String spConfig = configuration.getSpacePartitionerConfig();
		
		if(! spConfig.isEmpty()) {
			if(spConfig.contains("[") && spConfig.contains("]")) {
				return new Hyperrectangle(spConfig);	
			} else {
				logger.error("Got invalid space partitoner config {}", spConfig);
			}
		}
		
		return Hyperrectangle.createFullCoveringDimensionBoundingBox(configuration.getDimensions());
	}

	@Override
	public void splitFailed(final DistributionRegion sourceRegion, 
			final List<DistributionRegion> destination) throws BBoxDBException {
		
		try {
			distributionRegionZookeeperAdapter.setStateForDistributionRegion(sourceRegion, 
					DistributionRegionState.ACTIVE);
			
			for(final DistributionRegion childRegion : destination) {
				logger.info("Deleting child after failed split: {}", childRegion.getIdentifier());
				distributionRegionZookeeperAdapter.deleteChild(childRegion);
			}
			
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}

	@Override
	public void mergeFailed(final List<DistributionRegion> source, 
			final DistributionRegion destination) throws BBoxDBException {
		
		try {
			distributionRegionZookeeperAdapter.setStateForDistributionRegion(source.get(0).getParent(), 
					DistributionRegionState.SPLIT);
			
			for(final DistributionRegion childRegion : source) {
				distributionRegionZookeeperAdapter.setStateForDistributionRegion(childRegion, 
						DistributionRegionState.ACTIVE);
				
			}
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	@Override
	public void mergeComplete(final List<DistributionRegion> source, 
			final DistributionRegion destination) throws BBoxDBException {
		try {			
			for(final DistributionRegion childRegion : source) {
				logger.info("Merge done deleting: {}", childRegion.getIdentifier());
				distributionRegionZookeeperAdapter.deleteChild(childRegion);
			}
			
			distributionRegionZookeeperAdapter.setStateForDistributionRegion(destination, 
					DistributionRegionState.ACTIVE);
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	@Override
	public DistributionRegion getDestinationForMerge(List<DistributionRegion> source) 
			throws BBoxDBException {
		
		return source.get(0).getParent();
	}
	
	@Override
	public void splitComplete(final DistributionRegion sourceRegion, 
			final List<DistributionRegion> destination) throws BBoxDBException {
		
		try {
			distributionRegionZookeeperAdapter
				.setStateForDistributionRegion(sourceRegion, DistributionRegionState.SPLIT);
			
			// Children are ready
			for(final DistributionRegion childRegion : destination) {
				distributionRegionZookeeperAdapter
					.setStateForDistributionRegion(childRegion, DistributionRegionState.ACTIVE);
			}
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 * @throws InterruptedException 
	 */
	protected void waitUntilChildrenAreCreated(final DistributionRegion regionToSplit, 
			final int noOfChildren) throws InterruptedException {
		
		final Predicate<DistributionRegion> predicate = (r) -> r.getDirectChildren().size() == noOfChildren;
		DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);		
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 * @throws InterruptedException 
	 */
	@VisibleForTesting
	public void waitForSplitCompleteZookeeperCallback(final DistributionRegion regionToSplit, 
			final int noOfChildren) throws InterruptedException {
		
		final Predicate<DistributionRegion> predicate = (r) -> isSplitForNodeComplete(r, noOfChildren);
		DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);
	}
	
	/**
	 * Wait until the node state is
	 * @param region
	 * @param state
	 * @throws InterruptedException 
	 */
	@VisibleForTesting
	public void waitUntilNodeStateIs(final DistributionRegion region, final DistributionRegionState state) throws InterruptedException {
		final Predicate<DistributionRegion> predicate = (r) -> r.getState() == state;
		DistributionRegionSyncerHelper.waitForPredicate(predicate, region, distributionRegionSyncer);
	}

	/**
	 * Is the split for the given node complete?
	 * @param region
	 * @param noOfChildren 
	 * @return
	 */
	protected boolean isSplitForNodeComplete(final DistributionRegion region, final int noOfChildren) {
		
		if(region.getDirectChildren().size() != noOfChildren) {
			return false;
		}
		
		final boolean unreadyChild = region.getDirectChildren().stream()
			.anyMatch(r -> r.getState() != DistributionRegionState.REDISTRIBUTION_ACTIVE);
		
		return ! unreadyChild;
	}
	
	/**
	 * Set children to active and wait
	 * @param numberOfChilden2 
	 * @throws InterruptedException 
	 */
	protected void setStateToRedistributionActiveAndWait(final DistributionRegion regionToSplit, final int numberOfChilden) 
			throws ZookeeperException, InterruptedException {
		
		// update state
		for (final DistributionRegion region : regionToSplit.getAllChildren()) {
			final String childPath 
				= distributionRegionZookeeperAdapter.getZookeeperPathForDistributionRegion(region);

			distributionRegionZookeeperAdapter.setStateForDistributionGroup(childPath, 
					DistributionRegionState.REDISTRIBUTION_ACTIVE);
		}
		
		waitForSplitCompleteZookeeperCallback(regionToSplit, numberOfChilden);
	}
	

	@Override
	public List<List<DistributionRegion>> getMergeCandidates(final DistributionRegion distributionRegion) {
		
		final List<List<DistributionRegion>> result = new ArrayList<>();
		
		if(distributionRegion.getState() != DistributionRegionState.SPLIT) {
			return result;
		}
		
		final List<DistributionRegion> allChildren = distributionRegion.getAllChildren();
		final List<DistributionRegion> directChildren = distributionRegion.getDirectChildren();
		
		final int directChildrenSize = directChildren.size();
		
		// We have no children, no merge is possible
		if(directChildrenSize == 0) {
			return result;
		}
		
		// Do we have only direct children?
		if(allChildren.size() == directChildrenSize) {
			result.add(directChildren);
		}
		
		return result;
	}
	
	@Override
	public boolean isSplitable(final DistributionRegion distributionRegion) {
		return distributionRegion.isLeafRegion();
	}

}

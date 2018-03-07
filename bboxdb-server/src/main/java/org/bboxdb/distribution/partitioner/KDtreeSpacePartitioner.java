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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingBasedSplitStrategy;
import org.bboxdb.distribution.partitioner.regionsplit.SplitpointStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.region.DistributionRegionSyncerHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class KDtreeSpacePartitioner implements SpacePartitioner {

	/**
	 * The distribution group adapter
	 */
	private DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The name of the distribution group
	 */
	private DistributionGroupName distributionGroupName;

	/**
	 * The distribution region syncer
	 */
	private DistributionRegionSyncer distributionRegionSyncer;
	
	/**
	 * The space partitioner context
	 */
	private SpacePartitionerContext spacePartitionerContext;
	
	/**
	 * Is the space partitoner active?
	 */
	private volatile boolean active;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KDtreeSpacePartitioner.class);

	/**
	 * Reread and handle the dgroup version
	 * @throws ZookeeperException
	 */
	@Override
	public void init(final SpacePartitionerContext spacePartitionerContext) throws ZookeeperException {
		this.distributionGroupZookeeperAdapter = spacePartitionerContext.getDistributionGroupAdapter();
		this.distributionGroupName = spacePartitionerContext.getDistributionGroupName();
		this.spacePartitionerContext = spacePartitionerContext;
		this.active = true;
		
		TupleStoreConfigurationCache.getInstance().clear();
		DistributionGroupConfigurationCache.getInstance().clear();
		spacePartitionerContext.getDistributionRegionMapper().clear();
		
		this.distributionRegionSyncer = new DistributionRegionSyncer(spacePartitionerContext);
		
		logger.info("Root element for {} is deleted", distributionGroupName);
		
		if(distributionRegionSyncer != null) {
			distributionRegionSyncer.getDistributionRegionMapper().clear();
		}

		// Rescan tree
		distributionRegionSyncer.getRootNode();
	}
	
	/**
	 * Get the root node
	 * @return
	 * @throws BBoxDBException 
	 */
	@Override
	public DistributionRegion getRootNode() throws BBoxDBException {
	
		if(distributionRegionSyncer == null) {
			return null;
		}
		
		if(! active) {
			throw new BBoxDBException("Get root node on a non active space partitoner called");
		}

		return distributionRegionSyncer.getRootNode();
	}

	/**
	 * Split the node at the given position
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException 
	 * @throws ZookeeperNotFoundException 
	 */
	@Override
	public void splitRegion(final DistributionRegion regionToSplit, 
			final Collection<BoundingBox> samples) throws BBoxDBException {
		
		try {
			final SplitpointStrategy splitpointStrategy = new SamplingBasedSplitStrategy(samples);
			
			final int splitDimension = getSplitDimension(regionToSplit);
			final BoundingBox regionBox = regionToSplit.getConveringBox();
			final double splitPosition = splitpointStrategy.getSplitPoint(splitDimension, regionBox);
			
			splitNode(regionToSplit, splitPosition);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}
	
	@Override
	public void splitComplete(DistributionRegion regionToSplit) throws BBoxDBException {
		
		try {
			// Update zookeeer
			final DistributionGroupZookeeperAdapter zookeperAdapter 
				= ZookeeperClientFactory.getDistributionGroupAdapter();
			zookeperAdapter.setStateForDistributionRegion(regionToSplit, DistributionRegionState.SPLIT);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Split the node at the given split point
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws BBoxDBException
	 * @throws ResourceAllocationException 
	 */
	public void splitNode(final DistributionRegion regionToSplit, final double splitPosition)
			throws BBoxDBException, ResourceAllocationException {
		
		try {
			logger.debug("Write split at pos {} into zookeeper", splitPosition);
			final String parentPath = distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(regionToSplit);
			
			// Only one system executes the split, therefore we can determine the child ids
			
			// Calculate the covering bounding boxes
			final BoundingBox parentBox = regionToSplit.getConveringBox();
			final int splitDimension = getSplitDimension(regionToSplit);
			final BoundingBox leftBoundingBox = parentBox.splitAndGetLeft(splitPosition, splitDimension, true);
			final BoundingBox rightBoundingBox = parentBox.splitAndGetRight(splitPosition, splitDimension, false);
			
			final String fullname = distributionGroupName.getFullname();
			
			final String leftPath = distributionGroupZookeeperAdapter.createNewChild(parentPath, 0, 
					leftBoundingBox, fullname);
			
			final String rightPath = distributionGroupZookeeperAdapter.createNewChild(parentPath, 1, 
					rightBoundingBox, fullname);
			
			// Update state
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(parentPath, DistributionRegionState.SPLITTING);
			
			waitUntilChildrenAreCreated(regionToSplit);
	
			// Allocate systems (the data of the left node is stored locally)
			SpacePartitionerHelper.copySystemsToRegion(regionToSplit, regionToSplit.getDirectChildren().get(0), 
					this, distributionGroupZookeeperAdapter);
			
			SpacePartitionerHelper.allocateSystemsToRegion(regionToSplit.getDirectChildren().get(1), 
					this, regionToSplit.getSystems(), distributionGroupZookeeperAdapter);
			
			// update state
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(leftPath, DistributionRegionState.ACTIVE);
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(rightPath, DistributionRegionState.ACTIVE);	
	
			waitForSplitZookeeperCallback(regionToSplit);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}
	
	@Override
	public boolean isMergingSupported(final DistributionRegion distributionRegion) {
		return ! distributionRegion.isLeafRegion();
	}
	
	@Override
	public boolean isSplittingSupported(final DistributionRegion distributionRegion) {
		return distributionRegion.isLeafRegion();
	}

	@Override
	public void prepareMerge(final DistributionRegion regionToMerge) throws BBoxDBException {
		
		try {
			logger.debug("Merging region: {}", regionToMerge);
			final String zookeeperPath = distributionGroupZookeeperAdapter
					.getZookeeperPathForDistributionRegion(regionToMerge);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPath, 
					DistributionRegionState.ACTIVE);

			final List<DistributionRegion> childRegions = regionToMerge.getDirectChildren();
			
			for(final DistributionRegion childRegion : childRegions) {
				final String zookeeperPathChild = distributionGroupZookeeperAdapter
						.getZookeeperPathForDistributionRegion(childRegion);
				
				distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPathChild, 
					DistributionRegionState.MERGING);
			}
			
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	@Override
	public void mergeComplete(final DistributionRegion regionToMerge) throws BBoxDBException {
		try {
			final List<DistributionRegion> childRegions = regionToMerge.getDirectChildren();
			
			for(final DistributionRegion childRegion : childRegions) {
				logger.info("Merge done deleting: {}", childRegion.getIdentifier());
				distributionGroupZookeeperAdapter.deleteChild(childRegion);
			}
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	private void waitUntilChildrenAreCreated(final DistributionRegion regionToSplit) {
		final Predicate<DistributionRegion> predicate = (r) -> r.getDirectChildren().size() == 2;
		DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);		
	}
	
	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	private void waitForSplitZookeeperCallback(final DistributionRegion regionToSplit) {
		final Predicate<DistributionRegion> predicate = (r) -> isSplitForNodeComplete(r);
		DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);
	}
	
	/**
	 * Allocate the given list of systems to a region
	 * @param region
	 * @param allocationSystems
	 * @throws ZookeeperException
	 */
	@Override
	public void allocateSystemsToRegion(final DistributionRegion region, final Set<BBoxDBInstance> allocationSystems)
			throws ZookeeperException {
		
		final List<String> systemNames = allocationSystems.stream()
				.map(s -> s.getStringValue())
				.collect(Collectors.toList());
		
		logger.info("Allocating region {} to {}", region.getIdentifier(), systemNames);
		
		// Resource allocation successfully, write data to zookeeper
		for(final BBoxDBInstance instance : allocationSystems) {
			distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, instance);
		}
	}
	
	/**
	 * Is the split for the given node complete?
	 * @param region
	 * @return
	 */
	private boolean isSplitForNodeComplete(final DistributionRegion region) {
		
		if(region.getDirectChildren().size() != 2) {
			return false;
		}
		
		final boolean unreadyChild = region.getDirectChildren().stream()
			.anyMatch(r -> r.getState() != DistributionRegionState.ACTIVE);
		
		return ! unreadyChild;
	}
	
	/**
	 * Get the dimension of the distribution region
	 * @return
	 */
	private int getDimension() {
		try {
			final DistributionGroupConfigurationCache instance = DistributionGroupConfigurationCache.getInstance();
			final DistributionGroupConfiguration config = instance.getDistributionGroupConfiguration(distributionGroupName);
			return config.getDimensions();
		} catch (ZookeeperNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Returns the dimension of the split
	 * @return
	 */
	@VisibleForTesting
	public int getSplitDimension(final DistributionRegion distributionRegion) {
		return distributionRegion.getLevel() % getDimension();
	}

	/**
	 * Register a new callback
	 * @param callback
	 * @return
	 */
	@Override
	public boolean registerCallback(final DistributionRegionCallback callback) {
		return spacePartitionerContext.getCallbacks().add(callback);
	}
	
	/**
	 * Unregister a callback
	 * @param callback
	 * @return 
	 */
	@Override
	public boolean unregisterCallback(final DistributionRegionCallback callback) {
		return spacePartitionerContext.getCallbacks().remove(callback);
	}
	
	/** 
	 * Get the region id mapper
	 */
	@Override
	public DistributionRegionIdMapper getDistributionRegionIdMapper() {
		return spacePartitionerContext.getDistributionRegionMapper();
	}


	@Override
	public void shutdown() {
		logger.info("Shutdown space partitioner for instance {}", 
				spacePartitionerContext.getDistributionGroupName());
		
		this.active = false;
	}


	@Override
	public void splitFailed(final DistributionRegion region) throws BBoxDBException {
		try {
			distributionGroupZookeeperAdapter.setStateForDistributionRegion(region, 
					DistributionRegionState.ACTIVE);
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}


	@Override
	public void mergeFailed(final DistributionRegion regionToMerge) throws BBoxDBException {
		try {
			distributionGroupZookeeperAdapter.setStateForDistributionRegion(regionToMerge, 
					DistributionRegionState.ACTIVE);
			
			for(final DistributionRegion childRegion : regionToMerge.getDirectChildren()) {
				distributionGroupZookeeperAdapter.setStateForDistributionRegion(childRegion, 
						DistributionRegionState.ACTIVE);
				
			}
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
}

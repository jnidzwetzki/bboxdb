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

import java.util.List;
import java.util.function.Predicate;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.region.DistributionRegionSyncerHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public abstract class AbstractTreeSpacePartitoner implements SpacePartitioner {

	/**
	 * The distribution group adapter
	 */
	protected DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The zookeper client
	 */
	protected ZookeeperClient zookeeperClient;
	
	/**
	 * The name of the distribution group
	 */
	protected DistributionGroupName distributionGroupName;

	/**
	 * The distribution region syncer
	 */
	protected DistributionRegionSyncer distributionRegionSyncer;
	
	/**
	 * The space partitioner context
	 */
	protected SpacePartitionerContext spacePartitionerContext;
	
	/**
	 * Is the space partitoner active?
	 */
	protected volatile boolean active;
	
	/**
	 * Ignore the resource allocation exception (e.g. for testing in a stand alone environment)
	 */
	private boolean ignoreResouceAllocationException;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AbstractTreeSpacePartitoner.class);

	
	@Override
	public void init(final SpacePartitionerContext spacePartitionerContext) {
		this.zookeeperClient = spacePartitionerContext.getZookeeperClient();
		this.distributionGroupZookeeperAdapter = spacePartitionerContext.getDistributionGroupAdapter();
		this.distributionGroupName = spacePartitionerContext.getDistributionGroupName();
		this.spacePartitionerContext = spacePartitionerContext;
		this.active = true;
		this.ignoreResouceAllocationException = false;
		
		TupleStoreConfigurationCache.getInstance().clear();
		DistributionGroupConfigurationCache.getInstance().clear();
		spacePartitionerContext.getDistributionRegionMapper().clear();
	}
	
	@Override
	public void createRootNode(final DistributionGroupConfiguration configuration) throws BBoxDBException {
		try {
			final String distributionGroup 
				= spacePartitionerContext.getDistributionGroupName().getFullname();
			
			final String rootPath = 
					distributionGroupZookeeperAdapter.getDistributionGroupRootElementPath(distributionGroup);
			
			zookeeperClient.createDirectoryStructureRecursive(rootPath);
			
			final int nameprefix = distributionGroupZookeeperAdapter
					.getNextTableIdForDistributionGroup(distributionGroup);
						
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
					Integer.toString(nameprefix).getBytes());
			
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
					"".getBytes());
					
			distributionGroupZookeeperAdapter.setBoundingBoxForPath(rootPath, 
					BoundingBox.createFullCoveringDimensionBoundingBox(configuration.getDimensions()));

			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_REGION_STATE, 
					DistributionRegionState.ACTIVE.getStringValue().getBytes());		
			
			distributionGroupZookeeperAdapter.markNodeMutationAsComplete(rootPath);
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	@Override
	public DistributionRegion getRootNode() throws BBoxDBException {

		synchronized (this) {
			if(distributionRegionSyncer == null) {
				this.distributionRegionSyncer = new DistributionRegionSyncer(spacePartitionerContext);
				spacePartitionerContext.getDistributionRegionMapper().clear();
			}
		}

		
		if(! active) {
			throw new BBoxDBException("Get root node on a non active space partitoner called");
		}

		return distributionRegionSyncer.getRootNode();
	}

	@Override
	public boolean registerCallback(final DistributionRegionCallback callback) {
		return spacePartitionerContext.getCallbacks().add(callback);
	}

	@Override
	public boolean unregisterCallback(final DistributionRegionCallback callback) {
		return spacePartitionerContext.getCallbacks().remove(callback);
	}
	
	@Override
	public void shutdown() {
		logger.info("Shutdown space partitioner for instance {}", 
				spacePartitionerContext.getDistributionGroupName());
		
		this.active = false;
	}
	
	@Override
	public DistributionRegionIdMapper getDistributionRegionIdMapper() {
		return spacePartitionerContext.getDistributionRegionMapper();
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
	public void splitComplete(DistributionRegion regionToSplit) throws BBoxDBException {
		
		try {
			final DistributionGroupZookeeperAdapter zookeperAdapter 
				= ZookeeperClientFactory.getDistributionGroupAdapter();
			zookeperAdapter.setStateForDistributionRegion(regionToSplit, DistributionRegionState.SPLIT);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	protected void waitUntilChildrenAreCreated(final DistributionRegion regionToSplit, 
			final int noOfChildren) {
		
		final Predicate<DistributionRegion> predicate = (r) -> r.getDirectChildren().size() == noOfChildren;
		DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);		
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	protected void waitForSplitCompleteZookeeperCallback(final DistributionRegion regionToSplit, 
			final int noOfChildren) {
		
		final Predicate<DistributionRegion> predicate = (r) -> isSplitForNodeComplete(r, noOfChildren);
		DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);
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
			.anyMatch(r -> r.getState() != DistributionRegionState.ACTIVE);
		
		return ! unreadyChild;
	}
	
	/**
	 * Allocate systems to the children
	 * @param regionToSplit
	 * @param numberOfChilden
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 * @throws ZookeeperNotFoundException
	 */
	protected void allocateSystems(final DistributionRegion regionToSplit, final int numberOfChilden)
			throws ZookeeperException, ZookeeperNotFoundException, ResourceAllocationException {
		
		// Allocate systems (the data of the left node is stored locally)
		SpacePartitionerHelper.copySystemsToRegion(regionToSplit, 
				regionToSplit.getDirectChildren().get(0), distributionGroupZookeeperAdapter);
		
		for(int i = 1; i < numberOfChilden; i++) {
			final DistributionRegion region = regionToSplit.getDirectChildren().get(i);

			try {				
				SpacePartitionerHelper.allocateSystemsToRegion(region, 
						regionToSplit.getSystems(), distributionGroupZookeeperAdapter);
				
			} catch (ResourceAllocationException e) {
				if(! ignoreResouceAllocationException) {
					throw e;
				}
			}
		}
	}
	
	/**
	 * Ignore the resource allocation exception
	 * @param ignoreResouceAllocationException
	 */
	@VisibleForTesting
	public void setIgnoreResouceAllocationException(final boolean ignoreResouceAllocationException) {
		this.ignoreResouceAllocationException = ignoreResouceAllocationException;
	}
	
	/**
	 * Set children to active and wait
	 * @param numberOfChilden2 
	 */
	protected void setToActiveAndWait(final DistributionRegion regionToSplit, final int numberOfChilden) 
			throws ZookeeperException {
		
		// update state
		for (final DistributionRegion region : regionToSplit.getAllChildren()) {
			final String childPath 
				= distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(region);

			distributionGroupZookeeperAdapter.setStateForDistributionGroup(childPath, 
					DistributionRegionState.ACTIVE);
		}
		
		waitForSplitCompleteZookeeperCallback(regionToSplit, numberOfChilden);
	}

}

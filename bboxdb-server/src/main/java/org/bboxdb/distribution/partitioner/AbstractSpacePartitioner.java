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

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public abstract class AbstractSpacePartitioner implements SpacePartitioner{

	/**
	 * The distribution group adapter
	 */
	protected DistributionGroupAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The distribution group adapter
	 */
	protected DistributionRegionAdapter distributionRegionZookeeperAdapter;

	/**
	 * The zookeper client
	 */
	protected ZookeeperClient zookeeperClient;
	
	/**
	 * The name of the distribution group
	 */
	protected String distributionGroupName;
	
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
	 * The logger
	 */
	final static Logger logger = LoggerFactory.getLogger(AbstractSpacePartitioner.class);
	
	@Override
	public void init(final SpacePartitionerContext spacePartitionerContext) {
		this.zookeeperClient = spacePartitionerContext.getZookeeperClient();
		
		this.distributionGroupZookeeperAdapter 
			= spacePartitionerContext.getZookeeperClient().getDistributionGroupAdapter();
		
		this.distributionRegionZookeeperAdapter 
			= spacePartitionerContext.getZookeeperClient().getDistributionRegionAdapter();
		
		this.distributionGroupName = spacePartitionerContext.getDistributionGroupName();
		this.spacePartitionerContext = spacePartitionerContext;
		this.active = true;
		
		TupleStoreConfigurationCache.getInstance().clear();
		DistributionGroupConfigurationCache.getInstance().clear();
		spacePartitionerContext.getDistributionRegionMapper().clear();
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

		// The first child is stored on the same systems as the parent
		final DistributionRegion firstRegion = regionToSplit.getDirectChildren().get(0);
		
		final String firstRegionPath 
			= distributionRegionZookeeperAdapter.getZookeeperPathForDistributionRegion(firstRegion);
				
		SpacePartitionerHelper.copySystemsToRegion(regionToSplit.getSystems(), 
				firstRegionPath, zookeeperClient);

		final List<BBoxDBInstance> blacklistSystems = regionToSplit.getSystems();

		// For the remaining node, a new resource allocation is performed
		for(int i = 1; i < numberOfChilden; i++) {
			final DistributionRegion region = regionToSplit.getDirectChildren().get(i);
			
			final String path 
				= distributionRegionZookeeperAdapter.getZookeeperPathForDistributionRegion(region);
			
			final String fullname = region.getDistributionGroupName();
			
			SpacePartitionerHelper.allocateSystemsToRegion(path, fullname, 
					blacklistSystems, zookeeperClient);
		}
	}

	@Override
	public void prepareMerge(final List<DistributionRegion> source, 
			final DistributionRegion destination) throws BBoxDBException {
		
		try {			
			logger.debug("Merging region: {}", destination.getIdentifier());
			
			distributionRegionZookeeperAdapter.setStateForDistributionRegion(destination, 
					DistributionRegionState.REDISTRIBUTION_ACTIVE);
			
			for(final DistributionRegion childRegion : source) {
				final String zookeeperPathChild = distributionRegionZookeeperAdapter
						.getZookeeperPathForDistributionRegion(childRegion);
				
				distributionRegionZookeeperAdapter.setStateForDistributionGroup(zookeeperPathChild, 
					DistributionRegionState.MERGING);
			}			
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	@Override
	public void splitFailed(final DistributionRegion sourceRegion, final List<DistributionRegion> destination)
			throws BBoxDBException {
		
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

	@VisibleForTesting
	public DistributionRegionSyncer getDistributionRegionSyncer() {
		return distributionRegionSyncer;
	}
}

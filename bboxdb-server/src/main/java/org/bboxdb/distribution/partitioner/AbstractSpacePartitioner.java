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
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
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
	protected boolean ignoreResouceAllocationException;

	/**
	 * The logger
	 */
	final static Logger logger = LoggerFactory.getLogger(AbstractSpacePartitioner.class);
	
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
	 * Ignore the resource allocation exception
	 * @param ignoreResouceAllocationException
	 */
	@VisibleForTesting
	public void setIgnoreResouceAllocationException(final boolean ignoreResouceAllocationException) {
		this.ignoreResouceAllocationException = ignoreResouceAllocationException;
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
		SpacePartitionerHelper.copySystemsToRegion(regionToSplit, 
				regionToSplit.getDirectChildren().get(0), distributionGroupZookeeperAdapter);

		final List<BBoxDBInstance> blacklistSystems = regionToSplit.getSystems();

		// For the remaining node, a new resource allocation is performed
		for(int i = 1; i < numberOfChilden; i++) {
			final DistributionRegion region = regionToSplit.getDirectChildren().get(i);
			
			final String path 
				= distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(region);
			
			final String fullname = region.getDistributionGroupName().getFullname();
			
			makeResourceAllocation(path, fullname, blacklistSystems);
		}
	}

	/**
	 * Make a resource allocation
	 * @param region
	 * @param blacklistSystems
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws ResourceAllocationException
	 */
	protected void makeResourceAllocation(final String regionPath, 
			final String distributionGroupName,
			final List<BBoxDBInstance> blacklistSystems) throws ZookeeperException, 
			ZookeeperNotFoundException, ResourceAllocationException {
		
		try {
			SpacePartitionerHelper.allocateSystemsToRegion(regionPath, distributionGroupName, 
					blacklistSystems, distributionGroupZookeeperAdapter);
		} catch (ResourceAllocationException e) {
			if(! ignoreResouceAllocationException) {
				throw e;
			}
		}
	}

}

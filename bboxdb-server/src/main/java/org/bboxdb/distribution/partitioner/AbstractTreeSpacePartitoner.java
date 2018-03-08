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

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}

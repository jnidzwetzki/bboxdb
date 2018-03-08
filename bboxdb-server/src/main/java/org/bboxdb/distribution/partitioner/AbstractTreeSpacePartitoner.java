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

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTreeSpacePartitoner implements SpacePartitioner {

	/**
	 * The distribution group adapter
	 */
	protected DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
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
}

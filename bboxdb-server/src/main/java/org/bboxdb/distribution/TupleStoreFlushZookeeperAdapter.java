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
package org.bboxdb.distribution;

import java.util.function.BiConsumer;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleStoreFlushZookeeperAdapter implements BiConsumer<TupleStoreName, Long> {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreFlushZookeeperAdapter.class);
	
	@Override
	public void accept(final TupleStoreName ssTableName, final Long flushTimestamp) {
		
		// Fetch the local instance
		final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();
		final String distributionGroup = ssTableName.getDistributionGroup();

		try {			
			final SpacePartitioner spacepartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(distributionGroup);

			final DistributionRegion distributionGroupRoot = spacepartitioner.getRootNode();
			
			if(distributionGroupRoot == null) {
				logger.error("Root for {} is null, not executing callback", ssTableName);
				return;
			}
			
			final long regionId = ssTableName.getRegionId().getAsLong();
			
			final DistributionRegion distributionRegion 
				= DistributionRegionHelper.getDistributionRegionForNamePrefix(distributionGroupRoot, regionId);
		
			logger.debug("Updating checkpoint for: {} to {}", ssTableName.getFullname(), flushTimestamp);
			final DistributionRegionAdapter distributionRegionAdapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
			
			if(distributionRegionAdapter != null && distributionRegion != null) {
				distributionRegionAdapter.setCheckpointForDistributionRegion(distributionRegion, localInstance, flushTimestamp);
			}
			
		} catch (ZookeeperException | BBoxDBException e) {
			
			if(Thread.currentThread().isInterrupted()) {
				return;
			}
			
			logger.warn("Got an error while writing data to zookeeper for: " + distributionGroup);
			logger.debug("Full exception is", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

}

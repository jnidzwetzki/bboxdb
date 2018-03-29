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
package org.bboxdb.storage.sstable.compact;

import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactorHelper {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CompactorHelper.class);

	/**
	 * Is the region for the table active? 
	 * 
	 * @param tupleStoreName
	 * @return
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 */
	public static boolean isRegionActive(final TupleStoreName tupleStoreName) 
			throws StorageManagerException, InterruptedException {
		
		try {
			if(! tupleStoreName.isDistributedTable()) {
				logger.error("Tuple store {} is not a distributed table, untable to split", 
						tupleStoreName);
				return false;
			}
			
			final long regionId = tupleStoreName.getRegionId().getAsLong();
			
			final String distributionGroup = tupleStoreName.getDistributionGroup();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(distributionGroup);
			
			final DistributionRegion distributionRegion = spacePartitioner.getRootNode();

			final DistributionRegion regionToSplit = DistributionRegionHelper
					.getDistributionRegionForNamePrefix(distributionRegion, regionId);
			
			// Region does not exist
			if(regionToSplit == null) {
				logger.error("Unable to get distribution region {} {}", distributionRegion, regionId);
				return false;
			}
			
			if(regionToSplit.isRootElement()) {
				return true;
			}
		
			return regionToSplit.getState() == DistributionRegionState.ACTIVE;
		} catch (BBoxDBException e) {
			throw new StorageManagerException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		}
	}
}

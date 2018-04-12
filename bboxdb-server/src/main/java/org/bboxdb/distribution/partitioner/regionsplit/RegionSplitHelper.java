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
package org.bboxdb.distribution.partitioner.regionsplit;


import java.util.OptionalDouble;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionSplitHelper.class);
	
	/**
	 * Needs the region a split?
	 * @param region
	 * @return
	 * @throws BBoxDBException 
	 */
	public static boolean isRegionOverflow(final DistributionRegion region) throws BBoxDBException {
		
		// Is the data of the parent completely distributed?
		if(! isParentDataRedistributed(region)) {
			return false;
		}
		
		final OptionalDouble sizeOfRegionInMB = StatisticsHelper.getAndUpdateStatistics(region);

		if(! sizeOfRegionInMB.isPresent()) {
			return false;
		}
		
		try {			
			final long maxSize = getConfiguredRegionMaxSize(region);
			return (sizeOfRegionInMB.getAsDouble() > maxSize);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}
	
	/**
	 * Get maximal size of a region
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	private static long getConfiguredRegionMaxSize(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final String fullname = region.getDistributionGroupName();
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(fullname);

		return config.getMaximumRegionSize();
	}
	
	
	/**
	 * Is the data of the region parent completely redistributed, 
	 * if not, wait with local split
	 * @return
	 */
	private static boolean isParentDataRedistributed(final DistributionRegion region) {
		
		// Root region
		if(region.isRootElement()) {
			return true;
		}
		
		return region.getParent().getState() == DistributionRegionState.SPLIT;
	}

	/**
	 * Is the splitting of the region supported?
	 * @param regionToSplit
	 * @return
	 */
	public static boolean isSplittingSupported(final DistributionRegion region) {
		try {
			final String distributionGroupName = region.getDistributionGroupName();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroupName);

			return spacePartitioner.isSplitable(region);
		} catch (BBoxDBException e) {
			logger.error("Got exception while testing for merge", e);
			return false;
		}
	}
}

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

import java.util.List;
import java.util.Objects;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionMergeHelper {

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionMergeHelper.class);
	
	/**
	 * Get minimal size of a region
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	private static long getConfiguredRegionMinSizeInMB(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final String fullname = region.getDistributionGroupName().getFullname();
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(fullname);

		return config.getMinimumRegionSize();
	}
	
	/**
	 * Needs the region a merge?
	 * @param region
	 * @return
	 * @throws BBoxDBException 
	 */
	public static boolean isRegionUnderflow(final DistributionRegion region) throws BBoxDBException {
		
		// This might be the root region
		if(region == null || region.isRootElement()) {
			return false;
		}
		
		logger.info("Testing for underflow: {}", region.getRegionId());
		
		final List<DistributionRegion> children = region.getAllChildren();
		final boolean inactiveChilds = children.stream()
				.anyMatch(c -> c.getState() != DistributionRegionState.ACTIVE);
		
		if(inactiveChilds) {
			logger.info("Not all children ready, skip merge test for {} / {}", 
					region.getRegionId(), region.getAllChildren());
			return false;
		}
		
		// We are not responsible to this region
		final BBoxDBInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName();
		if(! region.getSystems().contains(localInstanceName)) {
			logger.info("Not testing for underflow for {} on {}", region.getRegionId(), localInstanceName);
			return false;
		}
		
		final double childRegionSize = getTotalRegionSize(region);
		if(childRegionSize == StatisticsHelper.INVALID_STATISTICS) {
			logger.info("Got invalid statistics for {}", region.getRegionId());
			return false;
		}
		
		try {
			final long minSize = getConfiguredRegionMinSizeInMB(region);
			
			logger.info("Testing for region {} underflow curent size is {} / min is {}", 
					region.getRegionId(), childRegionSize, minSize);
			
			return (childRegionSize < minSize);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Get the total size of the child regions
	 * @param region
	 * @return
	 */
	public static double getTotalRegionSize(final DistributionRegion region) {
		
		// Update statistics
		region.getDirectChildren().forEach(r -> StatisticsHelper.updateAverageStatistics(r));
		
		// Get statistics
		return region.getDirectChildren()
				.stream()
				.filter(Objects::nonNull)
				.filter(r -> StatisticsHelper.isEnoughHistoryDataAvailable(r.getIdentifier()))
				.mapToDouble(r -> StatisticsHelper.getAverageStatistics(r.getIdentifier()))
				.sum();
	}
	
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
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
	private static long getConfiguredRegionMinSizeInMB(final String fullname) 
			throws ZookeeperException, ZookeeperNotFoundException {
				
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
	public static boolean isRegionUnderflow(final List<DistributionRegion> sources) throws BBoxDBException {
		
		assert(! sources.isEmpty()) : "Sources can not be empty";

		final List<String> sourceIds = getRegionIdsFromRegionList(sources);
		
		logger.info("Testing for underflow: {}", sourceIds);
		
		final boolean inactiveChilds = sources.stream()
				.anyMatch(c -> c.getState() != DistributionRegionState.ACTIVE);
		
		if(inactiveChilds) {
			logger.info("Not all children ready, skip merge test for {}", sourceIds);
			return false;
		}
		
		// We are not responsible to this region
		final BBoxDBInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName();
		
		final boolean localSystemContained = sources
				.stream()
				.anyMatch(r -> r.getSystems().contains(localInstanceName));
		
		if(! localSystemContained) {
			logger.info("Not testing for underflow for {} on {}", sourceIds, localInstanceName);
			return false;
		}
		
		final double childRegionSize = getTotalRegionSize(sources);
		if(childRegionSize == StatisticsHelper.INVALID_STATISTICS) {
			logger.info("Got invalid statistics for {}", sourceIds);
			return false;
		}
		
		try {
			final String fullname = sources.get(0).getDistributionGroupName();
			final long minSize = getConfiguredRegionMinSizeInMB(fullname);
			
			logger.info("Testing for region {} underflow curent size is {} / min is {}", 
				sourceIds, childRegionSize, minSize);
			
			return (childRegionSize < minSize);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Get a list with the region ids
	 * @param sources
	 * @return
	 */
	private static List<String> getRegionIdsFromRegionList(final List<DistributionRegion> sources) {
		return sources.stream()
				.map(s -> s.getIdentifier())
				.collect(Collectors.toList());
	}

	/**
	 * Get the total size of the child regions
	 * @param region
	 * @return
	 */
	public static double getTotalRegionSize(final List<DistributionRegion> sources) {
		
		// Update statistics
		sources.forEach(r -> StatisticsHelper.updateAverageStatistics(r));
		
		// Get statistics
		return sources
				.stream()
				.filter(Objects::nonNull)
				.filter(r -> StatisticsHelper.isEnoughHistoryDataAvailable(r.getIdentifier()))
				.mapToDouble(r -> StatisticsHelper.getAverageStatistics(r.getIdentifier()))
				.sum();
	}
	
	/**
	 * Is the merging of the region supported?
	 * @param region
	 * @return
	 */
	public static boolean isMergingSupported(final DistributionRegion region) {

		if(! isMergingByZookeeperAllowed(region)) {
			logger.debug("Merging for region {} is not supported (Zookeeper)", region);
			return false;
		}

		if(! isMergingBySpacePartitionerAllowed(region)) {
			logger.debug("Merging for region {} is not supported (Space partitioner)", region);
			return false;
		}
		
		return true;
	}

	/**
	 * Is the merging by the space partitioner allowed
	 * @param region
	 * @return
	 */
	public static boolean isMergingBySpacePartitionerAllowed(final DistributionRegion region) {
		return getMergingCandidates(region).isEmpty() == false;
	}
	
	/**
	 * Is the merging by the space partitioner allowed?
	 * @param region
	 * @return
	 */
	public static List<List<DistributionRegion>> getMergingCandidates(final DistributionRegion region) {
		try {
			final String distributionGroupName = region.getDistributionGroupName();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroupName);

			return spacePartitioner.getMergeCandidates(region);
		} catch (BBoxDBException e) {
			logger.error("Got exception while testing for merge", e);
			return new ArrayList<>();
		}
	}

	/**
	 * Is the merging by zookeeper allowed?
	 * @param region
	 * @throws BBoxDBException
	 */
	public static boolean isMergingByZookeeperAllowed(final DistributionRegion region) {
		try {
			final DistributionGroupZookeeperAdapter groupZookeeperAdapter 
				= ZookeeperClientFactory.getDistributionGroupAdapter();
			
			return groupZookeeperAdapter.isMergingSupported(region);
		} catch (BBoxDBException e) {
			logger.error("Got exception while testing for merge", e);
			return false;
		}	
	}
	
}

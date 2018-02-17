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
import java.util.Map;
import java.util.Objects;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitHelper {
	
	/**
	 * The zookeeper adapter
	 */
	private final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionSplitHelper.class);
	
	public RegionSplitHelper() {
		this.distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	}

	/**
	 * Needs the region a split?
	 * @param region
	 * @return
	 * @throws BBoxDBException 
	 */
	public boolean isRegionOverflow(final DistributionRegion region) throws BBoxDBException {
		
		// Is the data of the parent completely distributed?
		if(! isParentDataRedistributed(region)) {
			return false;
		}
		
		try {
			final double sizeOfRegionInMB = getMaxRegionSizeFromStatistics(region);				
			final long maxSize = getRegionMaxSizeInMB(region);
			
			return (sizeOfRegionInMB > maxSize);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Get the max total size from the statistics map
	 * @param statistics
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public double getMaxRegionSizeFromStatistics(final DistributionRegion region) {
		
		try {
			final Map<BBoxDBInstance, Map<String, Long>> statistics 
				= distributionGroupZookeeperAdapter.getRegionStatistics(region);
			
			return statistics
				.values()
				.stream()
				.mapToDouble(p -> p.get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE))
				.filter(Objects::nonNull)
				.max()
				.orElse(0);
			
		} catch (Exception e) {
			logger.error("Got an exception while reading statistics", e);
			return 0;
		} 
	}
	
	/**
	 * Needs the region a merge?
	 * @param region
	 * @return
	 * @throws BBoxDBException 
	 */
	public boolean isRegionUnderflow(final DistributionRegion region) throws BBoxDBException {
		
		// This might be the root region
		if(region == null || region == DistributionRegion.ROOT_NODE_ROOT_POINTER) {
			return false;
		}
		
		final List<DistributionRegion> children = region.getAllChildren();
		final boolean inactiveChilds = children.stream()
				.anyMatch(c -> c.getState() != DistributionRegionState.ACTIVE);
		
		if(inactiveChilds) {
			logger.debug("Not all children ready, skip merge test");
			return false;
		}
		
		// We are not responsible to this region
		final BBoxDBInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName();
		if(! region.getSystems().contains(localInstanceName)) {
			logger.info("Not testing for underflow for {} on {}", region, localInstanceName);
			return false;
		}
		
		try {
			final double childRegionSize = getTotalRegionSize(region);
			final long minSize = getRegionMInSizeInMB(region);
			
			logger.info("Testing for region underflow curent size is {} / min is {} / children {}", 
					childRegionSize, minSize, region.getChildren());
			
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
	public double getTotalRegionSize(final DistributionRegion region) {
		return region.getChildren()
				.stream()
				.filter(Objects::nonNull)
				.mapToDouble(r -> getMaxRegionSizeFromStatistics(r))
				.sum();
	}
	
	/**
	 * Get maximal size of a region
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	private long getRegionMaxSizeInMB(final DistributionRegion region) throws ZookeeperException, ZookeeperNotFoundException {
		final String fullname = region.getDistributionGroupName().getFullname();
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(fullname);

		return config.getMaximumRegionSize();
	}
	
	/**
	 * Get minimal size of a region
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	private long getRegionMInSizeInMB(final DistributionRegion region) throws ZookeeperException, ZookeeperNotFoundException {
		final String fullname = region.getDistributionGroupName().getFullname();
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(fullname);

		return config.getMinimumRegionSize();
	}
	
	/**
	 * Is the data of the region parent completely redistributed, 
	 * if not, wait with local split
	 * @return
	 */
	protected boolean isParentDataRedistributed(final DistributionRegion region) {
		
		// Root region
		if(region.getParent() == DistributionRegion.ROOT_NODE_ROOT_POINTER) {
			return true;
		}
		
		return region.getParent().getState() == DistributionRegionState.SPLIT;
	}
}

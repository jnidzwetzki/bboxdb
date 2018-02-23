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


import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitHelper {

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionSplitHelper.class);
	
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
		
		final double sizeOfRegionInMB = StatisticsHelper.updateStatistics(region);

		if(sizeOfRegionInMB == StatisticsHelper.INVALID_STATISTICS) {
			return false;
		}
		
		try {			
			final long maxSize = getConfiguredRegionMaxSize(region);
			return (sizeOfRegionInMB > maxSize);
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
	private long getConfiguredRegionMaxSize(final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final String fullname = region.getDistributionGroupName().getFullname();
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(fullname);

		return config.getMaximumRegionSize();
	}
	
	
	/**
	 * Is the data of the region parent completely redistributed, 
	 * if not, wait with local split
	 * @return
	 */
	private boolean isParentDataRedistributed(final DistributionRegion region) {
		
		// Root region
		if(region.isRootElement()) {
			return true;
		}
		
		return region.getParent().getState() == DistributionRegionState.SPLIT;
	}
}

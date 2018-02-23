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

import java.util.Map;
import java.util.Objects;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsHelper {
	
	/**
	 * The value for invalid statistics
	 */
	public final static long INVALID_STATISTICS = 0;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(StatisticsHelper.class);
	
	/**
	 * The zookeeper adapter
	 */
	private final static DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	static {
		distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	}

	/**
	 * Get the max total size from the statistics map
	 * @param statistics
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public static double getMaxRegionSizeFromStatistics(final DistributionRegion region) {
		
		try {
			final Map<BBoxDBInstance, Map<String, Long>> statistics 
				= distributionGroupZookeeperAdapter.getRegionStatistics(region);
			
			return statistics
				.values()
				.stream()
				.mapToDouble(p -> p.get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE))
				.filter(Objects::nonNull)
				.max().orElse(INVALID_STATISTICS);
			
		} catch (Exception e) {
			logger.error("Got an exception while reading statistics", e);
			return INVALID_STATISTICS;
		} 
	}
}

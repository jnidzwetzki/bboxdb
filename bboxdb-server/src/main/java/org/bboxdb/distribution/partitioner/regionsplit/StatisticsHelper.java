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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Queue;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.EvictingQueue;

public class StatisticsHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StatisticsHelper.class);
	
	/**
	 * The zookeeper adapter
	 */
	private final static DistributionRegionAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The statistics history
	 */
	private final static Map<String, Queue<Double>> statisticsHistory;
	
	/**
	 * The statistics length
	 */
	public final static int HISTORY_LENGTH = 5;
	
	static {
		distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
		
		statisticsHistory = new HashMap<>();
	}

	/**
	 * Get the max total size from the statistics map
	 * @param statistics
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	public static OptionalDouble getAndUpdateStatistics(final DistributionRegion region) {
		
		try {
			final Map<BBoxDBInstance, Map<String, Long>> statistics 
				= distributionGroupZookeeperAdapter.getRegionStatistics(region);
			
			final OptionalDouble regionSize = statistics
				.values()
				.stream()
				.mapToDouble(p -> p.get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE))
				.filter(Objects::nonNull)
				.max();
			
			if(regionSize.isPresent()) {
				final String regionIdentifier = region.getIdentifier();
				updateStatisticsHistory(regionIdentifier, regionSize.getAsDouble());
			}
			
			return regionSize;
		} catch (Exception e) {
			logger.error("Got an exception while reading statistics", e);
			return OptionalDouble.empty();
		} 
	}

	/**
	 * Update the statistics 
	 * 
	 * @param region
	 * @param regionSize
	 */
	public static void updateStatisticsHistory(final String regionIdentifier, final double regionSize) {
		
		synchronized (statisticsHistory) {
			if(! statisticsHistory.containsKey(regionIdentifier)) {
				statisticsHistory.put(regionIdentifier, EvictingQueue.create(HISTORY_LENGTH));
			}
			
			statisticsHistory.get(regionIdentifier).add(regionSize);
		}

	}
	
	/**
	 * Get the average statistics
	 * 
	 * @param regionIdentifier
	 * @return
	 */
	public static double getAverageStatistics(final String regionIdentifier) {
		
		synchronized (statisticsHistory) {
			if(! statisticsHistory.containsKey(regionIdentifier)) {
				return 0;
			}
			
			final double statistics = statisticsHistory.get(regionIdentifier).stream()
				.mapToDouble(r -> r)
				.average().orElse(0);
						
			return statistics;
		}
	}
	
	/**
	 * Is enough history data available?
	 * @param regionIdentifier
	 * @return
	 */
	public static boolean isEnoughHistoryDataAvailable(final String regionIdentifier) {
		synchronized (statisticsHistory) {
			
			if(! statisticsHistory.containsKey(regionIdentifier)) {
				return false;
			}
			
			final int historySize = statisticsHistory.get(regionIdentifier).size();
								
			return historySize >= HISTORY_LENGTH;
		}
	}
	
	/**
	 * Delete all old statistics
	 */
	public static void clearHistory() {
		synchronized (statisticsHistory) {
			statisticsHistory.clear();
		}
	}
}

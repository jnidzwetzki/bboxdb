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

import java.util.HashMap;
import java.util.Map;

import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionIdMapperManager {
	
	/**
	 * The local mappings for a distribution group
	 */
	protected final static Map<DistributionGroupName, DistributionRegionIdMapper> instances;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionIdMapperManager.class);

	
	static {
		instances = new HashMap<DistributionGroupName, DistributionRegionIdMapper>();
	}
	
	/**
	 * Get the instance 
	 * @param distributionGroupName
	 */
	public static synchronized DistributionRegionIdMapper getInstance(final DistributionGroupName distributionGroupName) {
		if(! instances.containsKey(distributionGroupName)) {
			instances.put(distributionGroupName, new DistributionRegionIdMapper());
			
			// Read distribution group to generate the local mappings
			try {
				final SpacePartitioner partitioner 
					= SpacePartitionerCache.getSpacePartitionerForGroupName(
							distributionGroupName.getFullname());
				
				partitioner.getRootNode();
			} catch (ZookeeperException e) {
				logger.error("Got an expcetion by reading the space partitioner", e);
			}
			
		}
		
		return instances.get(distributionGroupName);
	}
}

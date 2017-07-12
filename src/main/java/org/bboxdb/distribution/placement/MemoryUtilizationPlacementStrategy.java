/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.distribution.placement;

import java.util.List;

import org.bboxdb.distribution.membership.DistributedInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multiset;

public class MemoryUtilizationPlacementStrategy extends LowUtilizationResourcePlacementStrategy {
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(MemoryUtilizationPlacementStrategy.class);
	
	public MemoryUtilizationPlacementStrategy() {

	}
	
	/**
	 * Get the system with the lowest memory / instance relation
	 * @param availableSystems
	 * @param systemUsage
	 * @return
	 * @throws ResourceAllocationException
	 */
	@Override
	protected DistributedInstance getSystemWithLowestUsage(final List<DistributedInstance> availableSystems, 
			final Multiset<DistributedInstance> systemUsage) throws ResourceAllocationException {
		
		DistributedInstance possibleSystem = null;
		double memoryUsageFactor = Double.MIN_VALUE;
		
		for(final DistributedInstance distributedInstance : availableSystems) {
			
			// Unknown = Empty instance
			if(systemUsage.count(distributedInstance) == 0) {
				return distributedInstance;
			}
			
			// unknown memory data
			if(distributedInstance.getMemory() <= 0) {
				continue;
			}
			
			if(possibleSystem == null) {
				possibleSystem = distributedInstance;
				memoryUsageFactor = calculateUsageFactor(systemUsage, distributedInstance);
			} else {
				if(calculateUsageFactor(systemUsage, distributedInstance) > memoryUsageFactor) {
					possibleSystem = distributedInstance;
					memoryUsageFactor = calculateUsageFactor(systemUsage, distributedInstance);
				}
			}
		}
		
		return possibleSystem;
	}

	/**
	 * Calculate the memory / instance usage factor
	 * @param systemUsage
	 * @param distributedInstance
	 * @return
	 */
	protected long calculateUsageFactor(final Multiset<DistributedInstance> systemUsage,
			final DistributedInstance distributedInstance) {
		return distributedInstance.getMemory() / systemUsage.count(distributedInstance);
	}

}

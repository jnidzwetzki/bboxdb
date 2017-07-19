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

import java.util.function.Predicate;

import org.bboxdb.distribution.membership.DistributedInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multiset;

public class MinRegionsResourcePlacementStrategy extends AbstractUtilizationPlacementStrategy {
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(MinRegionsResourcePlacementStrategy.class);
	
	public MinRegionsResourcePlacementStrategy() {

	}
	
	@Override
	protected Predicate<? super DistributedInstance> getUnusableSystemsFilterPredicate() {
		return i -> true;
	}


	@Override
	protected double calculateUsageFactor(Multiset<DistributedInstance> systemUsage,
			DistributedInstance distributedInstance) {
		
		final int usageCount = systemUsage.count(distributedInstance);
		
		if(usageCount == 0) {
			return 0;
		}
		
		// Lower utilization is preferred by the algorithm
		return (1 / usageCount);
	}

	
}

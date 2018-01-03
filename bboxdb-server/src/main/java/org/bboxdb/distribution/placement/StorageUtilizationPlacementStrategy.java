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
package org.bboxdb.distribution.placement;

import java.util.function.Predicate;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multiset;

public class StorageUtilizationPlacementStrategy extends AbstractUtilizationPlacementStrategy {
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(StorageUtilizationPlacementStrategy.class);
	
	public StorageUtilizationPlacementStrategy() {

	}
	
	/**
	 * Calculate the storages / instance usage factor
	 * @param systemUsage
	 * @param distributedInstance
	 * @return
	 */
	protected double calculateUsageFactor(final Multiset<BBoxDBInstance> systemUsage,
			final BBoxDBInstance distributedInstance) {
		
		final int count = systemUsage.count(distributedInstance);
		
		if(count == 0) {
			logger.error("Got an invalid count");
			return 0;
		}
		
		return distributedInstance.getNumberOfStorages() / count;
	}

	@Override
	protected Predicate<? super BBoxDBInstance> getUnusableSystemsFilterPredicate() {
		return i -> (i.getNumberOfStorages() > 0);
	}
}

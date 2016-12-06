/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.distribution.regionsplit;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.scalephant.storage.entity.FloatInterval;

public class SimpleSplitStrategy extends RegionSplitStrategy {

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(SimpleSplitStrategy.class);

	/**
	 * Perform a split of the given distribution region
	 */
	@Override
	protected void performSplit(final DistributionRegion region) {
		
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		final List<DistributedInstance> systems = distributedInstanceManager.getInstances();
		
		if(systems.isEmpty()) {
			logger.warn("Unable to split region, no ressources are avilable: " + region);
			return;
		}
		
		logger.info("Performing split of region: " + region);
		
		// Split region
		final int splitDimension = region.getSplitDimension();
		final FloatInterval interval = region.getConveringBox().getIntervalForDimension(splitDimension);
		
		logger.info("Split at dimension:" + splitDimension + " interval: " + interval);
		float midpoint = interval.getMidpoint();
		
		performSplitAtPosition(region, midpoint);
	}
}

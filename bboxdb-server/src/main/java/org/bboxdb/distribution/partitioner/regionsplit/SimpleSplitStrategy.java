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

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.storage.entity.DoubleInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSplitStrategy implements SplitpointStrategy {

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(SimpleSplitStrategy.class);

	/**
	 * Perform a split of the given distribution region
	 */
	@Override
	public double getSplitPoint(final DistributionRegion region) {
				
		logger.info("Performing split of region: {}", region);
		
		// Split region
		final int splitDimension = region.getSplitDimension();
		final DoubleInterval interval = region.getConveringBox().getIntervalForDimension(splitDimension);
		
		logger.info("Split at dimension:" + splitDimension + " interval: " + interval);
		return interval.getMidpoint();
	}
}

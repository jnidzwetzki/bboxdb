/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.tools.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.commons.math.Hyperrectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomQueryRangeGenerator {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RandomQueryRangeGenerator.class);
	
	/**
	 * Determine a random query rectangle
	 * @return
	 */
	public static Hyperrectangle getRandomQueryRange(final Hyperrectangle range, final double percentage) {
		
		logger.info("Generating a random hyperrectangle with a coverage of {} percent", percentage);
		
		final List<DoubleInterval> bboxIntervals = new ArrayList<>();
		
		// Determine query bounding box
		for(int dimension = 0; dimension < range.getDimension(); dimension++) {
			final double dataExtent = range.getExtent(dimension);
			final double randomDouble = ThreadLocalRandom.current().nextDouble();
			final double bboxOffset = (randomDouble % 1) * dataExtent;
			final double coordinateLow = range.getCoordinateLow(dimension);
			final double coordinateHigh = range.getCoordinateHigh(dimension);

			final double bboxStartPos = coordinateLow + bboxOffset;
			
			final double queryExtend = dataExtent * percentage;
			final double bboxEndPos = Math.min(bboxStartPos + queryExtend, coordinateHigh);

			final DoubleInterval doubleInterval = new DoubleInterval(bboxStartPos, bboxEndPos);
			bboxIntervals.add(doubleInterval);
		}
		
		return new Hyperrectangle(bboxIntervals);
	}

}

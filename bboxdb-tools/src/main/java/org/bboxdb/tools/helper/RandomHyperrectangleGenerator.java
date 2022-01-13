/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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

public class RandomHyperrectangleGenerator {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RandomHyperrectangleGenerator.class);
	
	/**
	 * Determine a random query rectangle
	 * @return
	 */
	public static Hyperrectangle generateRandomHyperrectangle(final Hyperrectangle completeSpace, final double percentage) {
		
		logger.debug("Generating a random hyperrectangle with a coverage of {} percent", percentage);
		
		final Hyperrectangle scaledRectangle = completeSpace.scaleVolumeByPercentage(percentage);
		
		if(scaledRectangle == null) {
			logger.error("Unable to scale {}", completeSpace);
			return completeSpace;
		}
		
		if(percentage <= 0 || percentage > 1) {
			logger.error("Unable to generate random space. Percentage has to be in interval (0, 1] (provided {})", percentage);
			return completeSpace;
		}
		
		final List<DoubleInterval> bboxIntervals = new ArrayList<>();
		
		// Determine query bounding box
		for(int dimension = 0; dimension < scaledRectangle.getDimension(); dimension++) {
			final double dataExtentCompleteRange = completeSpace.getExtent(dimension);
			
			final double extendRange = scaledRectangle.getExtent(dimension);

			// The offset can move the scaled rectangle
			final double offsetRage = dataExtentCompleteRange - extendRange;
			
			final double randomDouble = ThreadLocalRandom.current().nextDouble();
			final double bboxOffset = Math.abs((randomDouble % 1) * offsetRage);
						
			final DoubleInterval doubleInterval = new DoubleInterval(
					completeSpace.getCoordinateLow(dimension) + bboxOffset, 
					completeSpace.getCoordinateLow(dimension) + bboxOffset + extendRange);
						
			bboxIntervals.add(doubleInterval);
		}
		
		final Hyperrectangle generatedHyperrectangle = new Hyperrectangle(bboxIntervals);
		logger.debug("Complete space {} / generated {}", completeSpace, generatedHyperrectangle);
		
		return generatedHyperrectangle;
	}

}

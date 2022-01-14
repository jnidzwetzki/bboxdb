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
package org.bboxdb.commons.math;

import java.util.Optional;

public class HyperrectangleHelper {
	
	/**
	 * Parse the given bounding box
	 * @param bbox
	 * @return
	 */
	public static Optional<Hyperrectangle> parseBBox(final String bbox) {

		try {
			return Optional.of(Hyperrectangle.fromString(bbox));
		} catch (IllegalArgumentException e) {
			System.err.println("Invalid argument: " + bbox);
			return Optional.empty();
		}
	}
	
	/**
	 * Get a Hyperrectangle covering the full space
	 * @param dimensions
	 * @param min
	 * @param max
	 * @return
	 */
	public static Hyperrectangle getFullSpaceForDimension(final int dimensions, 
			final double min, final double max) {
		
		if(dimensions < 1) {
			throw new IllegalArgumentException("The dimensionality has to be at least 1");
		}
		
		if(min >= max) {
			throw new IllegalArgumentException("Min value has to be smaller than max value " 
					+ min + " " + max); 
		}
		
		final double fullSpaceValues[] = new double[2*dimensions];
		
		for(int i = 0; i < dimensions; i++) {
			fullSpaceValues[2*i] = min;
			fullSpaceValues[2*i+1] = max;
		}
		
		return new Hyperrectangle(fullSpaceValues);
	}
}

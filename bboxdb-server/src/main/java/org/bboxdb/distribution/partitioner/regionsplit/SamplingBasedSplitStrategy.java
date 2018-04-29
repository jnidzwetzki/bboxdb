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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.storage.StorageManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplingBasedSplitStrategy implements SplitpointStrategy {

	/**
	 * The samples
	 */
	private final Collection<Hyperrectangle> samples;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SamplingBasedSplitStrategy.class);

	public SamplingBasedSplitStrategy(final Collection<Hyperrectangle> samples) {
		this.samples = samples;
		assert (! samples.isEmpty()) : "Samples list is emoty";
	}

	@Override
	public double getSplitPoint(final int splitDimension, final Hyperrectangle coveringBox) 
			throws StorageManagerException {
		
		final List<Double> pointSamples = preprocessSamples(splitDimension, coveringBox);
		
		if(pointSamples.isEmpty()) {
			throw new StorageManagerException("Unable to determine split point, samples list is empty");
		}
		
		// Sort points
		pointSamples.sort((i1, i2) -> Double.compare(i1,i2));
		
		// Calculate point
		final int midpoint = pointSamples.size() / 2;
		final double splitPosition = pointSamples.get(midpoint);
		final double splitPositonRound = MathUtil.round(splitPosition, 5);
		
		return splitPositonRound;
	}

	/**
	 * Preprocess the samples
	 * @param splitDimension
	 * @param coveringBox
	 * @return
	 */
	private List<Double> preprocessSamples(final int splitDimension, final Hyperrectangle coveringBox) {
		
		final List<Double> pointSamples = new ArrayList<>();
		
		for(final Hyperrectangle sampleBox : samples) {
			// Add the begin and end pos to the lists, if the begin / end is in the 
			// covering box
			final DoubleInterval tupleInterval = sampleBox.getIntervalForDimension(splitDimension);
			final DoubleInterval groupInterval = coveringBox.getIntervalForDimension(splitDimension);
			
			if(groupInterval.isPointIncluded(tupleInterval.getBegin())) {
				pointSamples.add(tupleInterval.getBegin());
			}
			
			if(groupInterval.isPointIncluded(tupleInterval.getEnd())) {
				pointSamples.add(tupleInterval.getEnd());
			}
		}
		
		logger.info("Samples list has a size of {}, usable elements {}", 
				samples.size(), pointSamples.size());
		
		return pointSamples;
	}

}

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
package org.bboxdb.experiments.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.tools.TupleFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSampling implements Runnable {

	/**
	 * The file to import
	 */
	private final String filename;

	/**
	 * The format of the input file
	 */
	private final String format;

	/**
	 * The number of partitions
	 */
	private final int partitions;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StreamSampling.class);

	public StreamSampling(final String filename, final String format, 
			final int partitions) {
		this.filename = filename;
		this.format = format;
		this.partitions = partitions;
	}

	@Override
	public void run() {
		final List<Integer> sampleSizes = Arrays.asList(
			100, 250, 500, 750, 
			1000, 1250, 1500, 1750,
			2000, 2250, 2500, 2750,
			3000, 3250, 3500, 3750,
			4000, 4250, 4500, 4750,
			5000, 5250, 5500, 5750,
			6000, 6250, 6500, 6750,
			7000, 7250, 7500, 7750,
			8000, 8250, 8500, 8750,
			9000, 9250, 9500, 9750,
			1000, 10250, 10500, 10750
		);

		sampleSizes.forEach(s -> runExperiment(s));
	}

	/**
	 * Run the experiment with the given sample size
	 * @param sampleSize
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	protected void runExperiment(final int numberOfElements) {
		try {
			System.out.println("Simulating with sample size: " + numberOfElements);

			final List<Hyperrectangle> allSamples = getSamplesFromFile(numberOfElements);
			
			final Map<Hyperrectangle, List<Hyperrectangle>> activeRegions = new HashMap<>();
			final Map<Hyperrectangle, Integer> dimensions = new HashMap<>();
			
			final int sampleDimension = allSamples.get(0).getDimension();
			final Hyperrectangle fullSpace = Hyperrectangle.createFullCoveringDimensionBoundingBox(sampleDimension);
			
			activeRegions.put(fullSpace, allSamples);
			dimensions.put(fullSpace, 0);

			while(activeRegions.keySet().size() < partitions) {
				
				//System.out.format("We have now %s of %s active partitions, executing split %n", 
				//		activeRegions.size(), partitions);		
				
				// Get the region with the highest amount of contained samples
				final Hyperrectangle regionToSplit = activeRegions.entrySet()
					.stream()
					.max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1)
					.get().getKey();
				
				
				final List<Hyperrectangle> samples = activeRegions.get(regionToSplit);
				Collections.sort(samples);

				final int dimension = dimensions.get(regionToSplit);
				final Hyperrectangle splitHyperrectangle = samples.get(samples.size() / 2);
				final DoubleInterval splitPoint = splitHyperrectangle.getIntervalForDimension(dimension);
				final double midPoint = splitPoint.getMidpoint();
				
				System.out.format("Splitting region " + regionToSplit + " at " + midPoint);

				final Hyperrectangle leftRegion = regionToSplit.splitAndGetLeft(midPoint, dimension, true);
				final Hyperrectangle rightRegion = regionToSplit.splitAndGetRight(midPoint, dimension, false);

				final List<Hyperrectangle> newRegions = new ArrayList<>();
				newRegions.add(leftRegion);
				newRegions.add(rightRegion);
				
				final int nextDimension = (dimension + 1) % splitHyperrectangle.getDimension();
				dimensions.put(leftRegion, nextDimension);
				dimensions.put(rightRegion, nextDimension);

				// Redistribute samples
				newRegions.forEach(d -> activeRegions.put(d, new ArrayList<>()));
				final List<Hyperrectangle> oldSamples = activeRegions.remove(regionToSplit);
				
				for(final Hyperrectangle sample : oldSamples) {
					for(final Hyperrectangle region : newRegions) {
						if(region.intersects(sample)) {
							activeRegions.get(region).add(sample);
						}
					}
				}
				
				activeRegions.remove(regionToSplit);
			}
		} catch (ClassNotFoundException e) {
			logger.error("Got exception during experiment", e);
		} catch (IOException e) {
			logger.error("Got exception during experiment", e);
		}
	}
	
	
	/**
	 * Run the experiment for the given position
	 * @param splitPos
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	protected List<Hyperrectangle> getSamplesFromFile(final int numberOfSamples) throws ClassNotFoundException, IOException {

		final List<Hyperrectangle> samples = new ArrayList<>();
		
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);

		tupleFile.addTupleListener(t -> {
			final Hyperrectangle polygonBoundingBox = t.getBoundingBox();
			samples.add(polygonBoundingBox);
	    });

		try {
			tupleFile.processFile(numberOfSamples);
		} catch (Exception e) {
			logger.error("Got an Exception while reading file", e);
			System.exit(-1);
		}

		return samples;
	}


	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {

		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <format> <partitons>");
			System.exit(-1);
		}

		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		final String partitionsString = Objects.requireNonNull(args[2]);

		final int partitons = MathUtil.tryParseIntOrExit(partitionsString);

		final StreamSampling determineSamplingSize = new StreamSampling(filename, format, partitons);
		determineSamplingSize.run();
	}

}

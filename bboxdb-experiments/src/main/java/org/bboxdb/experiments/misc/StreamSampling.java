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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.tools.RandomSamplesReader;
import org.bboxdb.tools.TupleFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.Stats;

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
		
		performSamplingBasedPartitioning();
		
		// Element based partitioning
		final List<Integer> sampleSizes = Arrays.asList(
			100, 500,
			1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 
			10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000,
			20000, 21000, 22000, 23000, 24000, 25000, 26000, 27000, 28000, 29000,
			30000, 31000, 32000, 33000, 34000, 35000, 36000, 37000, 38000, 39000,
			40000, 41000, 42000, 43000, 44000, 45000, 46000, 47000, 48000, 49000,
			50000
		);

		sampleSizes.forEach(s -> runExperiment(s));

	}

	/**
	 * Perform a sampling based partitoning
	 * @param fli
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private void performSamplingBasedPartitioning() {
	
		
		try {
			final List<Hyperrectangle> takenBoxes = RandomSamplesReader.readSamplesRandom(filename, format, 0.1);
			System.out.println("Read samples: " + takenBoxes.size());
			final Set<Hyperrectangle> activePartitions = partitionSpace(takenBoxes);
			performPartitoning(activePartitions);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
			
			final Set<Hyperrectangle> activePartitions = partitionSpace(allSamples);
			performPartitoning(activePartitions);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Perform the experiment with the given partitioning
	 * @param activePartitions
	 */
	private void performPartitoning(final Set<Hyperrectangle> activePartitions) {
		
		final Map<Hyperrectangle, AtomicLong> buckets = new HashMap<>();
		for(final Hyperrectangle bucket : activePartitions) {
			buckets.put(bucket, new AtomicLong(0));
		}
		
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);

		tupleFile.addTupleListener(t -> {
			final Hyperrectangle polygonBoundingBox = t.getBoundingBox();
			
			for(final Entry<Hyperrectangle, AtomicLong> entry : buckets.entrySet()) {
				if(polygonBoundingBox.intersects(entry.getKey())) {
					entry.getValue().incrementAndGet();
				}
			}
		});

		try {
			tupleFile.processFile();
		} catch (Exception e) {
			logger.error("Got an Exception while reading file", e);
			System.exit(-1);
		}
		

		final ArrayList<AtomicLong> elements = new ArrayList<>(buckets.values());

		IntStream.range(0, elements.size()).forEach(
				i -> System.out.format("%d\t%d%n", i, elements.get(i).get())
		);
		
		
		final Stats experimentStats = Stats.of(elements);
		System.out.println("Min: " + experimentStats.min());
		System.out.println("Max: " + experimentStats.max());
		System.out.println("Mean: " + experimentStats.mean());
		System.out.println("StdDev: " + experimentStats.populationStandardDeviation());
	}

	/**
	 * Partition the space
	 * @param numberOfElements
	 * @return
	 */
	private Set<Hyperrectangle> partitionSpace(final List<Hyperrectangle> allSamples) {
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
							
			System.out.println("Splitting region " + regionToSplit + " at " + midPoint);

			
			Hyperrectangle leftRegion;
			Hyperrectangle rightRegion;
			try {
				leftRegion = regionToSplit.splitAndGetLeft(midPoint, dimension, true);
				rightRegion = regionToSplit.splitAndGetRight(midPoint, dimension, false);
			} catch (Exception e) {
				logger.error("Unable to split, ignoring", e);
				break;
			}

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
		
		return activeRegions.keySet();
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

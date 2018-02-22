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
package org.bboxdb.experiments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.tools.TupleFileReader;

public class TestKDTreeSplit implements Runnable {

	/**
	 * The file to import
	 */
	protected final Map<String, String> filesAndFormats;

	/**
	 * The elements
	 */
	protected final Map<BoundingBox, List<BoundingBox>> elements;
	
	/**
	 * Box dimensions
	 */
	protected final Map<BoundingBox, Integer> boxDimension;

	/**
	 * The sampling size
	 */
	protected final static double SAMPLING_SIZE = 1d;

	/**
	 * The random for our samples
	 */
	protected final Random random;

	/**
	 * The dimension of the input data
	 */
	protected int dataDimension = -1;
	
	/**
	 * Experiment sizes
	 */
	protected final List<Integer> experimentSize;
	
	public TestKDTreeSplit(Map<String, String> filesAndFormats, List<Integer> experimentSize) {
		this.filesAndFormats = filesAndFormats;
		this.experimentSize = experimentSize;
		this.elements = new HashMap<>();
		this.boxDimension = new HashMap<>();
		this.random = new Random(System.currentTimeMillis());
	}

	@Override
	public void run() {
		experimentSize.forEach(e -> runExperiment(e));
	}

	/**
	 * Run the experiment with the given sample size
	 * @param sampleSize
	 * @throws IOException 
	 */
	protected void runExperiment(final int maxRegionSize) {
		System.out.println("# Simulating with max element size: " + maxRegionSize);
		
		elements.clear();	
		boxDimension.clear();
		
		for(Entry<String, String> elements : filesAndFormats.entrySet()) {
			final String filename = elements.getKey();
			final String format = elements.getValue();
			
			System.out.println("Processing file: " + filename);
			final TupleFileReader tupleFile = new TupleFileReader(filename, format);
			
			tupleFile.addTupleListener(t -> {
				insertNextBoundingBox(t.getBoundingBox(), maxRegionSize);
			});
			
			try {
				tupleFile.processFile();
			} catch (IOException e) {
				System.err.println("Got an IOException during experiment: "+ e);
				System.exit(-1);
			}
		}
		
		// Print results
		final List<Integer> buckets = elements.values()
				.stream()
				.map(l -> l.size())
				.collect(Collectors.toList());
		
		IntStream.range(0, buckets.size()).forEach(i -> System.out.format("%d\t%d\n", i, buckets.get(i)));
	}

	/**
	 * Handle the next bounding box
	 * @param maxRegionSize 
	 * @param tuple
	 */
	protected void insertNextBoundingBox(final BoundingBox boundingBox, 
			final int maxRegionSize) {
		
		// Create first entry
		if(elements.isEmpty()) {
			dataDimension = boundingBox.getDimension();
			final BoundingBox coveringBoundingBox = BoundingBox.createFullCoveringDimensionBoundingBox(dataDimension);
			elements.put(coveringBoundingBox, new ArrayList<>());
			boxDimension.put(coveringBoundingBox, 0);
		}
		
		// Add element to all needed bounding boxes
		elements.entrySet()
			.stream()
			.filter(e -> e.getKey().overlaps(boundingBox))
			.forEach(e -> e.getValue().add(boundingBox));
		
		final Predicate<Entry<BoundingBox, List<BoundingBox>>> boxFullPredicate 
			= e -> e.getValue().size() >= maxRegionSize;
		
		// Split and remove full boxes
		final List<BoundingBox> boxesToSplit = elements.entrySet()
			.stream()
			.filter(boxFullPredicate)
			.map(e -> e.getKey())
			.collect(Collectors.toList());
		
		// Split region
		boxesToSplit.forEach(e -> splitRegion(e));
		
		// Remove split regions
		elements.entrySet().removeIf(e -> boxesToSplit.contains(e.getKey()));
	}
	
	/**
	 * Split the region
	 * @param sampleSize
	 * @param numberOfElements 
	 * @return 
	 */
	protected void splitRegion(final BoundingBox boundingBoxToSplit) {
		
		final int parentBoxDimension = boxDimension.get(boundingBoxToSplit) % dataDimension;

		final double splitPosition = getSplitPosition(boundingBoxToSplit, parentBoxDimension);
		
		final BoundingBox leftBBox = boundingBoxToSplit.splitAndGetLeft(splitPosition, 
				parentBoxDimension, true);
		final BoundingBox rightBBox = boundingBoxToSplit.splitAndGetRight(splitPosition, 
				parentBoxDimension, false);
		
		// Data to redistribute
		final List<BoundingBox> dataToRedistribute = elements.get(boundingBoxToSplit);
		
		// Write the box dimension
		boxDimension.put(leftBBox, parentBoxDimension + 1);
		boxDimension.put(rightBBox, parentBoxDimension + 1);
		
		// Insert new boxes and remove old one
		elements.put(leftBBox, new ArrayList<>());
		elements.put(rightBBox, new ArrayList<>());
		elements.remove(boundingBoxToSplit);
		
		dataToRedistribute.forEach(b -> {
			if(leftBBox.overlaps(b)) {
				elements.get(leftBBox).add(b);
			}
			
			if(rightBBox.overlaps(b)) {
				elements.get(rightBBox).add(b);
			}	
		});
	}

	/***
	 * Calculate the split position
	 * @param boundingBoxToSplit
	 * @return
	 */
	protected double getSplitPosition(final BoundingBox boundingBoxToSplit, final int dimension) {
		final List<Double> pointSamples = new ArrayList<>();
		final Set<Integer> takenSamples = new HashSet<>();
		final List<BoundingBox> elementsToProcess = elements.get(boundingBoxToSplit);
		
		final int numberOfElements = elementsToProcess.size();
		final long numberOfSamples = (long) (numberOfElements / 100.0 * SAMPLING_SIZE);

		double sample = 0;
		
		// Try to find n samples (= 2n points)
		while(pointSamples.size() < (2 * numberOfSamples)) {
			sample++;
			final int sampleId = Math.abs(random.nextInt()) % numberOfElements;
			
			if(takenSamples.contains(sampleId)) {
				continue;
			}
			
			takenSamples.add(sampleId);
			
			final BoundingBox bboxSample = elementsToProcess.get(sampleId);
			
			if(bboxSample.getCoordinateLow(dimension) > boundingBoxToSplit.getCoordinateLow(dimension)) {
				pointSamples.add(bboxSample.getCoordinateLow(dimension));
			}	
			
			if(bboxSample.getCoordinateHigh(dimension) < boundingBoxToSplit.getCoordinateHigh(dimension)) {
				pointSamples.add(bboxSample.getCoordinateHigh(dimension));
			}	
						
			// Unable to find enough samples
			if(sample > (50 * numberOfSamples)) {
				break;
			}			
		}
				
		pointSamples.sort((b1, b2) -> Double.compare(b1, b2));
		
		return pointSamples.get(pointSamples.size() / 2);
	}
	
	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length < 2) {
			System.err.println("Usage: programm <size1,size2,sizeN> <filename1:format1> <filenameN:formatN> <filenameN:formatN>");
			System.exit(-1);
		}
		
		final String experimentSizeString = Objects.requireNonNull(args[0]);
		final List<Integer> experimentSize = Arrays.asList(experimentSizeString.split(","))
				.stream()
				.map(e -> MathUtil.tryParseIntOrExit(e))
				.collect(Collectors.toList());

		final Map<String, String> filesAndFormats = new HashMap<>();

		for(int pos = 1; pos < args.length; pos++) {
			
			final String element = args[pos];
			
			if(! element.contains(":")) {
				System.err.println("Element does not contain format specifier: " + element);
				System.exit(-1);
			}
			
			final String[] splitFile = element.split(":");
			
			if(splitFile.length != 2) {
				System.err.println("Unable to get two elements after format split: " + element);
				System.exit(-1);
			}

			filesAndFormats.put(splitFile[0], splitFile[1]);
		}
		
		final TestKDTreeSplit testSplit = new TestKDTreeSplit(filesAndFormats, experimentSize);
		testSplit.run();
	}

}

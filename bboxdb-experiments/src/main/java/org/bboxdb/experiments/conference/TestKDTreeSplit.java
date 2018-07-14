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
package org.bboxdb.experiments.conference;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.io.FileUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.tools.TupleFileReader;

import com.google.common.io.Files;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class TestKDTreeSplit implements Runnable {

	/**
	 * The file to import
	 */
	private final Map<String, String> filesAndFormats;

	/**
	 * The elements
	 */
	private final Map<Hyperrectangle, Database> elements;

	/**
	 * Box dimensions
	 */
	private final Map<Hyperrectangle, Integer> boxDimension;

	/**
	 * The sampling size
	 */
	private final static double SAMPLING_SIZE = 1d;

	/**
	 * The dimension of the input data
	 */
	private int dataDimension = -1;

	/**
	 * Experiment sizes
	 */
	private final List<Integer> experimentSize;

	/**
	 * The dbEnv
	 */
	private final Environment dbEnv;

	/**
	 * The DB config
	 */
	private DatabaseConfig dbConfig;

	public TestKDTreeSplit(final Map<String, String> filesAndFormats, final List<Integer> experimentSize) {
		this.filesAndFormats = filesAndFormats;
		this.experimentSize = experimentSize;
		this.elements = new HashMap<>();
		this.boxDimension = new HashMap<>();
	
		final File folder = Files.createTempDir();
		FileUtil.deleteDirOnExit(folder.toPath());
		
		final EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);
	    envConfig.setSharedCache(true);
		dbEnv = new Environment(folder, envConfig);

		dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);		
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
	private void runExperiment(final int maxRegionSize) {
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
		final List<Long> buckets = elements.values()
				.stream()
				.map(d -> d.count())
				.collect(Collectors.toList());

		IntStream.range(0, buckets.size()).forEach(i -> System.out.format("%d\t%d\n", i, buckets.get(i)));
	}
	
	

	/**
	 * Handle the next bounding box
	 * @param maxRegionSize
	 * @param tuple
	 */
	private void insertNextBoundingBox(final Hyperrectangle boundingBox,
			final int maxRegionSize) {

		// Create first entry
		if(elements.isEmpty()) {
			dataDimension = boundingBox.getDimension();
			final Hyperrectangle coveringBoundingBox = Hyperrectangle.createFullCoveringDimensionBoundingBox(dataDimension);
			final Database database = buildNewDatabase();
			elements.put(coveringBoundingBox, database);
			boxDimension.put(coveringBoundingBox, 0);
		}
		
		// Bounding box db entry
		final DatabaseEntry key = new DatabaseEntry();
		key.setData(Long.toString(System.nanoTime()).getBytes());
		final DatabaseEntry value = new DatabaseEntry();
		value.setData(boundingBox.toByteArray());
		
		// Add element to all needed bounding boxes
		elements.entrySet()
			.stream()
			.filter(e -> e.getKey().overlaps(boundingBox))
			.forEach(e -> e.getValue().put(null, key, value));

		final Predicate<Entry<Hyperrectangle, Database>> boxFullPredicate
			= e -> e.getValue().count() >= maxRegionSize;

		// Split and remove full boxes
		final List<Hyperrectangle> boxesToSplit = elements.entrySet()
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
	 * @return
	 */
	private Database buildNewDatabase() {
		final Database database = dbEnv.openDatabase(null, Long.toString(System.nanoTime()), dbConfig);
		return database;
	}

	/**
	 * Split the region
	 * @param sampleSize
	 * @param numberOfElements
	 * @return
	 */
	private void splitRegion(final Hyperrectangle boundingBoxToSplit) {

		final int parentBoxDimension = boxDimension.get(boundingBoxToSplit) % dataDimension;

		final double splitPosition = getSplitPosition(boundingBoxToSplit, parentBoxDimension);

		final Hyperrectangle leftBBox = boundingBoxToSplit.splitAndGetLeft(splitPosition,
				parentBoxDimension, true);
		
		final Hyperrectangle rightBBox = boundingBoxToSplit.splitAndGetRight(splitPosition,
				parentBoxDimension, false);

		// Write the box dimension
		boxDimension.put(leftBBox, parentBoxDimension + 1);
		boxDimension.put(rightBBox, parentBoxDimension + 1);

		// Insert new boxes and remove old one
		elements.put(leftBBox, buildNewDatabase());
		elements.put(rightBBox, buildNewDatabase());
		
		final Database database = elements.remove(boundingBoxToSplit);

		// Data to redistribute
	    final Cursor cursor = database.openCursor(null, null);
	    
	    final DatabaseEntry foundKey = new DatabaseEntry();
	    final DatabaseEntry foundData = new DatabaseEntry();

	    while(cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	        final Hyperrectangle box = Hyperrectangle.fromByteArray(foundData.getData());
	        
	        if(leftBBox.overlaps(box)) {
				elements.get(leftBBox).put(null, foundKey, foundData);
			}

			if(rightBBox.overlaps(box)) {
				elements.get(rightBBox).put(null, foundKey, foundData);
			}
	    }
	    
	    cursor.close();
	    database.close();
	    
	    dbEnv.removeDatabase(null, database.getDatabaseName());
	}

	/***
	 * Calculate the split position
	 * @param boundingBoxToSplit
	 * @return
	 */
	private double getSplitPosition(final Hyperrectangle boundingBoxToSplit, final int dimension) {
		final List<Double> pointSamples = new ArrayList<>();
		final Database database = elements.get(boundingBoxToSplit);

		final long numberOfElements = database.count();
		final long numberOfSamples = (long) (numberOfElements / 100.0 * SAMPLING_SIZE);

	    final Cursor cursor = database.openCursor(null, null);
	    
	    final DatabaseEntry foundKey = new DatabaseEntry();
	    final DatabaseEntry foundData = new DatabaseEntry();
	    
	    long elementNumber = 0;
	    
		// Try to find n samples (= 2n points)
	    while(cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	    	
			elementNumber++;

	    		if(elementNumber % (2 * numberOfSamples) == 0) {
	    			continue;
	    		}
			
	        final Hyperrectangle bboxSample = Hyperrectangle.fromByteArray(foundData.getData());

			if(bboxSample.getCoordinateLow(dimension) > boundingBoxToSplit.getCoordinateLow(dimension)) {
				pointSamples.add(bboxSample.getCoordinateLow(dimension));
			}

			if(bboxSample.getCoordinateHigh(dimension) < boundingBoxToSplit.getCoordinateHigh(dimension)) {
				pointSamples.add(bboxSample.getCoordinateHigh(dimension));
			}	
		}

	    cursor.close();
	    
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

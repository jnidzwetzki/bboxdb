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
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.client.tools.TupleListFutureStore;

import com.google.common.base.Stopwatch;

public class TestBoundingBoxQuery implements Runnable {

	/**
	 * The file to import
	 */
	protected final String filename;
	
	/**
	 * The format of the input file
	 */
	protected String format;
	
	/**
	 * The endpoint name
	 */
	protected String endpoint;
	
	/**
	 * The cluster name
	 */
	protected String cluster;
	
	/**
	 * The tablename to query
	 */
	protected final String tablename;
	
	/**
	 * The random for our samples
	 */
	protected final Random random;

	/**
	 * The amount of queries
	 */
	protected final static int QUERIES = 1000;
	
	/**
	 * The number of experiment retries
	 */
	protected final static int RETRIES = 5;
	
	/**
	 * The pending futures
	 */
	protected final TupleListFutureStore pendingFutures;

	public TestBoundingBoxQuery(final String filename, final String format, 
			final String endpoint, String cluster, String tablename) {
		this.filename = filename;
		this.format = format;
		this.endpoint = endpoint;
		this.cluster = cluster;
		this.tablename = tablename;
		this.pendingFutures = new TupleListFutureStore();
		this.random = new Random(System.currentTimeMillis());
	}
	
	@Override
	public void run() {
		System.out.format("# Reading %s\n", filename);
		final BoundingBox boundingBox = ExperimentHelper.determineBoundingBox(filename, format);
		
		System.out.println("Connecting to BBoxDB cluster");
		final BBoxDB bboxDBConnection = new BBoxDBCluster(endpoint, cluster);
		
		if(! bboxDBConnection.connect()) {
			System.err.println("Unable to connect to the BBoxDB cluster, exiting");
			System.exit(-1);
		}

		final List<Double> experimentSize = Arrays.asList(0.001, 0.01, 0.1, 1.0);
		experimentSize.forEach(e -> runExperiment(e, boundingBox, bboxDBConnection));
		
		pendingFutures.shutdown();
	}

	/**
	 * Run the experiment with the max dimension size
	 * @param boundingBox 
	 * @param bboxDBConnection 
	 * @param sampleSize
	 * @throws BBoxDBException 
	 * @throws IOException 
	 */
	protected void runExperiment(final double maxDimensionSize, final BoundingBox boundingBox, 
			final BBoxDB bboxDBConnection) {
		
		try {
			System.out.println("# Simulating with max dimension size: " + maxDimensionSize);
			long totalElapsedTime = 0;
			for(int execution = 0; execution < RETRIES; execution++) {
				final Stopwatch stopwatch = Stopwatch.createStarted();
				executeQueries(maxDimensionSize, boundingBox, bboxDBConnection);
				final long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
				System.out.println("#" + elapsedTime);
				totalElapsedTime += elapsedTime;
			}
			
			System.out.println(maxDimensionSize + "\t" + totalElapsedTime / RETRIES);
		} catch (Exception e) {
			System.err.println("Got exception while executing experiment: " + e);
			System.exit(-1);
		}
	}

	/**
	 * Execute the bounding box queries
	 * @param maxDimensionSize
	 * @param boundingBox
	 * @param bboxDBConnection 
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 * @throws RejectedException 
	 */
	protected void executeQueries(final double maxDimensionSize, final BoundingBox boundingBox, 
			final BBoxDB bboxDBConnection) throws BBoxDBException, InterruptedException, RejectedException {
		
		for(int i = 0; i < QUERIES; i++) {
			
			final List<DoubleInterval> bboxIntervals = new ArrayList<>();
			// Determine query bounding box
			for(int dimension = 0; dimension < boundingBox.getDimension(); dimension++) {
				final double dataExtent = boundingBox.getExtent(dimension);
				final double bboxOffset = (random.nextDouble() % 1) * dataExtent;
				final double coordinateLow = boundingBox.getCoordinateLow(dimension);
				final double coordinateHigh = boundingBox.getCoordinateHigh(dimension);

				final double bboxStartPos = coordinateLow + bboxOffset;
				final double bboxEndPos = Math.min(bboxStartPos + dataExtent * maxDimensionSize, coordinateHigh);
				
				final DoubleInterval doubleInterval = new DoubleInterval(bboxStartPos, bboxEndPos);
				bboxIntervals.add(doubleInterval);
			}
			
			final BoundingBox queryBox = new BoundingBox(bboxIntervals);
			final TupleListFuture future = bboxDBConnection.queryBoundingBox(tablename, queryBox);
			pendingFutures.put(future);
		}
		
		pendingFutures.waitForCompletion();
	}
	
	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws IOException {
		
		// Check parameter
		if(args.length != 5) {
			System.err.println("Usage: programm <filename> <format> <endpoint> <cluster> <table>");
			System.exit(-1);
		}
		
		final String filename = Objects.requireNonNull(args[0]);
		final String format = Objects.requireNonNull(args[1]);
		final String endpoint = Objects.requireNonNull(args[2]);
		final String cluster = Objects.requireNonNull(args[3]);
		final String table = Objects.requireNonNull(args[4]);
		
		final TestBoundingBoxQuery experiment = new TestBoundingBoxQuery(filename, format, endpoint, 
				cluster, table);
		
		experiment.run();
	}

}

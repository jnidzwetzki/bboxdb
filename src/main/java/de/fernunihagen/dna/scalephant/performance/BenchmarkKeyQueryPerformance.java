/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.performance;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.client.ScalephantException;
import de.fernunihagen.dna.scalephant.network.client.future.EmptyResultFuture;
import de.fernunihagen.dna.scalephant.network.client.future.TupleResultFuture;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class BenchmarkKeyQueryPerformance extends AbstractBenchmark {

	/**
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);

	/**
	 * The amount of tuples to insert
	 */
	protected final int tuplesToInsert;

	/** 
	 * A 3 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
	 */
	protected final static String DISTRIBUTION_GROUP = "3_testgroup3";
	
	/** 
	 * A 3 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
	 */
	protected final static String TABLE = DISTRIBUTION_GROUP + "_testdata";

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BenchmarkKeyQueryPerformance.class);

	
	public BenchmarkKeyQueryPerformance(final int tuplesToInsert) {
		super();
		this.tuplesToInsert = tuplesToInsert;
	}

	
	@Override
	protected void prepare() throws Exception {
		super.prepare();
		
		// Remove old data
		final EmptyResultFuture deleteResult = scalephantClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteResult.waitForAll();
		
		// Create a new distribution group
		final EmptyResultFuture createResult = scalephantClient.createDistributionGroup(DISTRIBUTION_GROUP, (short) 3);
		createResult.waitForAll();
		
		logger.info("Inserting " + tuplesToInsert + " tuples");
	
		// Insert the tuples
		for(; insertedTuples.get() < tuplesToInsert; insertedTuples.incrementAndGet()) {
			scalephantClient.insertTuple(TABLE, new Tuple(Integer.toString(insertedTuples.get()), BoundingBox.EMPTY_BOX, "abcdef".getBytes()));
		}
		
		// Wait for requests to settle
		logger.info("Wait for insert requests to settle");
		while(scalephantClient.getInFlightCalls() != 0) {
			logger.info(scalephantClient.getInFlightCalls() + " are pending");
			Thread.sleep(1000);
		}
		logger.info("All insert requests are settled");
	}
	
	@Override
	protected void startBenchmarkTimer() {
		// Set the benchmark time
		startTime = System.currentTimeMillis();
		System.out.println("#Iteration\tTime");
	}
	
	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException, ScalephantException {
	
		for(int i = 0; i < 100; i++) {
			final long start = System.nanoTime();
			final TupleResultFuture result = scalephantClient.queryKey(TABLE, Integer.toString(40));
			
			// Wait for request to settle
			for(int requestId = 0; requestId < result.getNumberOfResultObjets(); requestId++) {
				final Object queryResult = result.get(requestId);
				if(! (queryResult instanceof Tuple)) {
					logger.warn("Query failed");
				}
			}
			
			final long end = System.nanoTime();
			System.out.println(i + "\t" + (end-start));
			
			Thread.sleep(1000);
		}
	}
	
	@Override
	protected DataTable getDataTable() {
		// We don't need a data model for this benchmark
		return null;
	}

	/* ====================================================
	 * Main
	 * ====================================================
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final int[] tupleAmount = {10000, 50000, 100000, 50000, 1000000, 500000};
		
		for(final int tuples : tupleAmount) {
			final BenchmarkKeyQueryPerformance benchmarkInsertPerformance = new BenchmarkKeyQueryPerformance(tuples);
			benchmarkInsertPerformance.run();
		}
	}
	
}

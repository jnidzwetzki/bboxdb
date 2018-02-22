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
package org.bboxdb.tools.benchmark;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;

public class BenchmarkInsertPerformance extends AbstractBenchmark {

	/**
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);
	
	/** 
	 * A 3 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
	 */
	protected final static String DISTRIBUTION_GROUP = "testgroup3";
	
	/** 
	 * A 3 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
	 */
	protected final static String TABLE = DISTRIBUTION_GROUP + "_testdata";

	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException, BBoxDBException {

		// Number of tuples
		final int tuples = 5000000;
				
		// Remove old data
		final EmptyResultFuture deleteResult = bboxdbClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteResult.waitForAll();
		
		// Create a new distribution group
		final DistributionGroupConfiguration config = DistributionGroupConfigurationBuilder.create(3)
				.withReplicationFactor((short) 3)
				.build();
		
		final EmptyResultFuture createResult = bboxdbClient.createDistributionGroup(DISTRIBUTION_GROUP, 
				config);
		
		createResult.waitForAll();
		
		final Random bbBoxRandom = new Random();
	
		// Insert the tuples
		for(; insertedTuples.get() < tuples; insertedTuples.incrementAndGet()) {
			final double x = Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			final double y = Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			final double z = Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			
			final BoundingBox boundingBox = new BoundingBox(x, x+1, y, y+1, z, z+1);
			
			final EmptyResultFuture insertFuture = bboxdbClient.insertTuple(TABLE, new Tuple(Integer.toString(insertedTuples.get()), boundingBox, "abcdef".getBytes()));
			
			// register pending future
			pendingFutures.put(insertFuture);
		}
	}

	@Override
	protected DataTable getDataTable() {
		return new DataTable() {
			
			protected int lastInsertedTuples = 0;
			protected int diff = 0;
			
			@Override
			public String getValueForColum(short colum) {
				switch (colum) {
				
				// Total amount of inserted tuples
				case 0:
					final int tuples = insertedTuples.get();
					diff = tuples - lastInsertedTuples;
					lastInsertedTuples = tuples;
					return Integer.toString(tuples);
					
				// Diff amount of inserted tuples
				case 1:
					return Integer.toString(diff);

				default:
					return "-----";
				}
			}
			
			@Override
			public String getTableHeader() {
				return "#Time\tTuples\tTuples_per_sec";
			}
			
			@Override
			public short getColumns() {
				return 2;
			}
		};
	}	
	
	/* ====================================================
	 * Main
	 * ====================================================
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final BenchmarkInsertPerformance benchmarkInsertPerformance = new BenchmarkInsertPerformance();
		benchmarkInsertPerformance.run();
	}
	
}

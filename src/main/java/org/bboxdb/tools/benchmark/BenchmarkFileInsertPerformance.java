/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.osm.util.Polygon;
import org.bboxdb.tools.converter.osm.util.SerializerHelper;

public class BenchmarkFileInsertPerformance extends AbstractBenchmark {

	/**
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);
	
	/** 
	 * A 2 dimensional distribution group 
	 */
	protected final static String DISTRIBUTION_GROUP = "2_osmgroup";
	
	/**
	 * The name of the table to insert data into
	 */
	protected final String table;
	
	/**
	 * The replication factor
	 */
	protected final short replicationFactor;
	
	/**
	 * The filename to parse
	 */
	protected final String filename;

	/**
	 * The serializer
	 */
	protected final SerializerHelper<Polygon> serializerHelper = new SerializerHelper<>();

	public BenchmarkFileInsertPerformance(final String filename, final short replicationFactor) {
		this.filename = filename;
		this.table = DISTRIBUTION_GROUP + "_" + System.currentTimeMillis();
		this.replicationFactor = replicationFactor;
	}

	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException, BBoxDBException {
	
		try (
				final BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
			) {

		    String line;
		    while ((line = reader.readLine()) != null) {
		    	final Polygon polygon = Polygon.fromGeoJson(line);
		    	final byte[] tupleBytes = polygon.toGeoJson().getBytes();
		    
				final Tuple tuple = new Tuple(Long.toString(polygon.getId()), polygon.getBoundingBox(), tupleBytes);
				final EmptyResultFuture insertFuture = bboxdbClient.insertTuple(table, tuple);
				
				// register pending future
				pendingFutures.add(insertFuture);
				checkForCompletedFutures();
				
				insertedTuples.incrementAndGet();
		    }
		} catch (BBoxDBException | IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void prepare() throws Exception {
		super.prepare();

		// Remove old data
		final EmptyResultFuture deleteResult = bboxdbClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteResult.waitForAll();
		
		// Create a new distribution group
		final EmptyResultFuture createResult = bboxdbClient.createDistributionGroup(DISTRIBUTION_GROUP, replicationFactor);
		createResult.waitForAll();
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
	public static void main(final String[] args) throws InterruptedException, ExecutionException {
		
		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <replication factor>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String replicationFactorString = args[1];
		short replicationFactor = -1;
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(-1);
		}
		
		try {
			replicationFactor = Short.parseShort(replicationFactorString);
		} catch(NumberFormatException e) {
			System.err.println("Invalid replication factor: " + replicationFactorString);
			System.exit(-1);
		}
		
		final BenchmarkFileInsertPerformance benchmarkInsertPerformance = new BenchmarkFileInsertPerformance(filename, replicationFactor);
		benchmarkInsertPerformance.run();
	}
}

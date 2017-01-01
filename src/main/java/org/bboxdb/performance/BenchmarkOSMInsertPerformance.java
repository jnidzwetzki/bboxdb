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
package org.bboxdb.performance;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.performance.osm.OSMFileReader;
import org.bboxdb.performance.osm.OSMStructureCallback;
import org.bboxdb.performance.osm.util.GeometricalStructure;
import org.bboxdb.performance.osm.util.SerializerHelper;
import org.bboxdb.storage.entity.Tuple;

public class BenchmarkOSMInsertPerformance extends AbstractBenchmark implements OSMStructureCallback  {

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
	 * The type to import
	 */
	protected final String type;

	/**
	 * The serializer
	 */
	protected final SerializerHelper<GeometricalStructure> serializerHelper = new SerializerHelper<>();

	public BenchmarkOSMInsertPerformance(final String filename, final String type, final short replicationFactor) {
		this.filename = filename;
		this.type = type;
		this.table = DISTRIBUTION_GROUP + "_" + type;
		this.replicationFactor = replicationFactor;
	}

	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException, BBoxDBException {
		final OSMFileReader osmFileReader = new OSMFileReader(filename, type, this);
		osmFileReader.run();		
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

	/**
	 * Callback from OSM reader
	 */
	@Override
	public void processStructure(final GeometricalStructure geometricalStructure) {
		try {
			final byte[] tupleBytes = serializerHelper.toByteArray(geometricalStructure);
			final Tuple tuple = new Tuple(Long.toString(geometricalStructure.getId()), geometricalStructure.getBoundingBox(), tupleBytes);
			final EmptyResultFuture insertFuture = bboxdbClient.insertTuple(table, tuple);
			
			// register pending future
			pendingFutures.add(insertFuture);
			checkForCompletedFutures();
			
			insertedTuples.incrementAndGet();
		} catch (IOException | BBoxDBException e) {
			e.printStackTrace();
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
	public static void main(final String[] args) throws InterruptedException, ExecutionException {
		
		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <" + OSMFileReader.getFilterNames() + "> <replication factor>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String type = args[1];
		final String replicationFactorString = args[2];
		short replicationFactor = -1;
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(-1);
		}
		
		// Check type
		if(! OSMFileReader.getAllFilter().contains(type)) {
			System.err.println("Unknown type: " + type);
			System.exit(-1);
		}
		
		try {
			replicationFactor = Short.parseShort(replicationFactorString);
		} catch(NumberFormatException e) {
			System.err.println("Invalid replication factor: " + replicationFactorString);
			System.exit(-1);	
		}
		
		final BenchmarkOSMInsertPerformance benchmarkInsertPerformance = new BenchmarkOSMInsertPerformance(filename, type, replicationFactor);
		benchmarkInsertPerformance.run();
	}


	
}

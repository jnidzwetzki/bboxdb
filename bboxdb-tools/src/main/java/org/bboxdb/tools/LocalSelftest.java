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
package org.bboxdb.tools;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSelftest {

	/**
	 * The destination table
	 */
	protected final static String TABLENAME = "testgroup_testtable";
	
	/**
	 * The amount of tuples 
	 */
	protected final static int TUPLES = 1000000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(LocalSelftest.class);
	
	public static void main(final String[] args) throws InterruptedException, ExecutionException, BBoxDBException, StorageManagerException, RejectedException {
		
		if(args.length != 1) {
			logger.error("Usage: LocalSelftest <Iterations>");
			System.exit(-1);
		}
		
		try {
			final int iterations = Integer.parseInt(args[0]);
			logger.info("Running selftest......");
			
			final TupleStoreManagerRegistry storageRegistry = new TupleStoreManagerRegistry();
			storageRegistry.init();
			
			final TupleStoreName sstable = new TupleStoreName(TABLENAME);
			final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(sstable);

			for(int iteration = 0; iteration < iterations; iteration++) {
				logger.info("Running iteration {}", iteration);
				storageRegistry.deleteTable(sstable);
				testInsertDelete(storageManager);
			}
			
			storageRegistry.shutdown();
			logger.info("Selftest done");

		} catch(NumberFormatException e) {
			logger.error("Unable to parse {} as a number, exiting", args[0]);
			System.exit(-1);
		}	
	}


	/**
	 * Test the creation and the deletion of a big amount of tuples
	 * @param storageManager
	 * @param iteration
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws RejectedException 
	 */
	protected static void testInsertDelete(final TupleStoreManager storageManager) throws StorageManagerException, InterruptedException, RejectedException {

		logger.info("Inserting tuples...");
		for(int i = 0; i < TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		Thread.sleep(1000);
		
		logger.info("Deleting tuples...");
		for(int i = 0; i < TUPLES; i++) {
			storageManager.delete(Integer.toString(i), MicroSecondTimestampProvider.getNewTimestamp());
		}
		
		for(int iteration = 0; iteration < 4; iteration++) {
			logger.info("Query deleted keys ({})...", iteration);
			// Fetch the deleted tuples
			for(int i = 0; i < TUPLES; i++) {
				final List<Tuple> resultTuples = storageManager.get(Integer.toString(i));
				
				assert (resultTuples.isEmpty()) : "List is not empty";
			}
		}
		Thread.sleep(1000);
	}
}

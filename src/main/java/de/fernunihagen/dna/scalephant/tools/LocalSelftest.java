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
package de.fernunihagen.dna.scalephant.tools;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.client.ScalephantException;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

public class LocalSelftest {

	/**
	 * The destination table
	 */
	protected final static String TABLENAME = "2_testgroup_testtable";
	
	/**
	 * The amount of tuples 
	 */
	protected final static int TUPLES = 1000000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(LocalSelftest.class);
	


	public static void main(final String[] args) throws InterruptedException, ExecutionException, ScalephantException, StorageManagerException {
		
		if(args.length != 1) {
			logger.error("Usage: LocalSelftest <Iterations>");
			System.exit(-1);
		}
		
		try {
			final int iterations = Integer.parseInt(args[0]);
			logger.info("Running selftest......");
			
			final SSTableManager storageManager = StorageRegistry.getSSTableManager(new SSTableName(TABLENAME));

			for(int iteration = 0; iteration < iterations; iteration++) {
				logger.info("Running iteration {}", iteration);
				storageManager.clear();
				
				testInsertDelete(storageManager);
			}
			
			storageManager.shutdown();
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
	 */
	protected static void testInsertDelete(final SSTableManager storageManager) throws StorageManagerException, InterruptedException {

		logger.info("Inserting tuples...");
		for(int i = 0; i < TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		Thread.sleep(1000);
		
		logger.info("Deleting tuples...");
		for(int i = 0; i < TUPLES; i++) {
			storageManager.delete(Integer.toString(i), System.currentTimeMillis());
		}
		
		// Let the storage manager swap the memtables out
		Thread.sleep(10000);
		
		logger.info("Query deleted keys...");
		// Fetch the deleted tuples
		for(int i = 0; i < TUPLES; i++) {
			final Tuple resultTuple2 = storageManager.get(Integer.toString(i));
			Assert.assertEquals(null, resultTuple2);
		}
		
		Thread.sleep(1000);
	}
}

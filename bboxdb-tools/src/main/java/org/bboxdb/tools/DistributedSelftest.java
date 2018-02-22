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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class DistributedSelftest {

	/**
	 * The name of the distribution group
	 */
	private static final String DISTRIBUTION_GROUP = "testgroup";
	
	/**
	 * The table to query
	 */
	private static final String TABLE = DISTRIBUTION_GROUP + "_mytable";
	
	/**
	 * The amount of operations
	 */
	private static final int NUMBER_OF_OPERATIONS = 10000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributedSelftest.class);


	public static void main(final String[] args) throws InterruptedException, ExecutionException, BBoxDBException {
		
		if(args.length < 2) {
			logger.error("Usage: DistributedSelftest <Cluster-Name> <Cluster-Endpoint1> <Cluster-EndpointN>");
			System.exit(-1);
		}

		logger.info("Running selftest......");
		
		final String clustername = args[0];
		final Collection<String> endpoints = new ArrayList<String>();
		for(int i = 1; i < args.length; i++) {
			endpoints.add(args[i]);
		}
	
		final BBoxDBCluster bboxdbCluster = new BBoxDBCluster(endpoints, clustername); 
		bboxdbCluster.connect();
		
		if(! bboxdbCluster.isConnected()) {
			logger.error("Connection could not be established");
			System.exit(-1);
		}
		
		logger.info("Connected to cluster: " + clustername);
		logger.info("With endpoint(s): " + endpoints);
		
		recreateDistributionGroup(bboxdbCluster);
		
		executeSelftest(bboxdbCluster);
	}

	/**
	 * Recreate the distribution group	
	 * @param bboxdbCluster
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static void recreateDistributionGroup(final BBoxDBCluster bboxdbCluster) throws BBoxDBException, InterruptedException, ExecutionException {
		logger.info("Delete old distribution group: " + DISTRIBUTION_GROUP);
		final EmptyResultFuture deleteFuture = bboxdbCluster.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteFuture.waitForAll();
		if(deleteFuture.isFailed()) {
			logger.error("Unable to delete distribution group: " + DISTRIBUTION_GROUP);
			logger.error(deleteFuture.getAllMessages());
			System.exit(-1);
		}
		
		// Wait for distribution group to settle
		Thread.sleep(5000);
		
		logger.info("Create new distribution group: " + DISTRIBUTION_GROUP);
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder.create(2)
				.withReplicationFactor((short) 2)
				.build();
		
		final EmptyResultFuture createFuture = bboxdbCluster.createDistributionGroup(DISTRIBUTION_GROUP, 
				configuration);
		
		createFuture.waitForAll();
		if(createFuture.isFailed()) {
			logger.error("Unable to create distribution group: " + DISTRIBUTION_GROUP);
			logger.error(createFuture.getAllMessages());
			System.exit(-1);
		}
		
		// Wait for distribution group to appear
		Thread.sleep(5000);
	}

	/**
	 * Execute the selftest
	 * @param bboxdbClient
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws BBoxDBException 
	 */
	private static void executeSelftest(final BBoxDBCluster bboxdbClient) throws InterruptedException, ExecutionException, BBoxDBException {
		final Random random = new Random();
		long iteration = 1;
		
		while(true) {
			logger.info("Starting new iteration: " + iteration);
			insertNewTuples(bboxdbClient);
			queryForExistingTuplesByKey(bboxdbClient, random);
			queryForExistingTuplesByTime(bboxdbClient);
			deleteTuples(bboxdbClient);
			queryForNonExistingTuples(bboxdbClient);
			
			Thread.sleep(1000);
			
			iteration++;
		}
	}

	/**
	 * Execute a time query
	 * @param bboxdbClient
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	private static void queryForExistingTuplesByTime(final BBoxDBCluster bboxdbClient) throws InterruptedException, ExecutionException, BBoxDBException {
		logger.info("Executing time query");
		
		final TupleListFuture queryResult = bboxdbClient.queryVersionTime(TABLE, 0);
		queryResult.waitForAll();
		
		if(queryResult.isFailed()) {
			logger.error("Time query result is failed");
			logger.error(queryResult.getAllMessages());
			System.exit(-1);
		}
		
		final int totalTuples = Iterators.size(queryResult.iterator());
		
		if(totalTuples != NUMBER_OF_OPERATIONS) {
			logger.error("Got {} tuples back, but expected {}", totalTuples, NUMBER_OF_OPERATIONS);
			System.exit(-1);
		}
	}

	/**
	 * Delete the stored tuples
	 * @param bboxdbClient
	 * @throws InterruptedException
	 * @throws BBoxDBException 
	 * @throws ExecutionException 
	 */
	private static void deleteTuples(final BBoxDBCluster bboxdbClient) throws InterruptedException, BBoxDBException, ExecutionException {
		logger.info("Deleting tuples");
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(i);
			final EmptyResultFuture deletionResult = bboxdbClient.deleteTuple(TABLE, key);
			deletionResult.waitForAll();
			
			if(deletionResult.isFailed() ) {
				logger.error("Got an error while deleting: {} ", key);
				logger.error(deletionResult.getAllMessages());
				System.exit(-1);
			}
		}
	}

	/**
	 * Query for the stored tuples
	 * @param bboxdbClient
	 * @param random
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws BBoxDBException 
	 */
	private static void queryForExistingTuplesByKey(final BBoxDBCluster bboxdbClient, final Random random) throws InterruptedException, ExecutionException, BBoxDBException {
		logger.info("Query for tuples");
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(Math.abs(random.nextInt()) % NUMBER_OF_OPERATIONS);
			final TupleListFuture queryResult = bboxdbClient.queryKey(TABLE, key);
			queryResult.waitForAll();
			
			if(queryResult.isFailed()) {
				logger.error("Query {} : Got failed future, when query for: {}", i, key);
				logger.error(queryResult.getAllMessages());
				System.exit(-1);
			}
			
			boolean tupleFound = false;
			
			for(final Tuple tuple : queryResult) {
				if(! tuple.getKey().equals(key)) {
					logger.error("Query {}: Got tuple with wrong key.", i);
					logger.error("Expected: {} but got: {}", i, tuple.getKey());
					System.exit(-1);
				}
				
				tupleFound = true;
			}
			
			if(tupleFound == false) {
				logger.error("Query {}: Key {} not found", i, key);
				System.exit(-1);
			}
		}
	}
	
	/**
	 * Query for non existing tuples and exit, as soon as a tuple is found
	 * @param bboxdbClient
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static void queryForNonExistingTuples(final BBoxDBCluster bboxdbClient) throws BBoxDBException, InterruptedException, ExecutionException {
		logger.info("Query for non existing tuples");
		
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(i);
	
			final TupleListFuture queryResult = bboxdbClient.queryKey(TABLE, key);
			queryResult.waitForAll();
			
			if(queryResult.isFailed()) {
				logger.error("Query {}: Got failed future, when query for: {}", i, key);
				logger.error(queryResult.getAllMessages());
				System.exit(-1);
			}
			
			for (final Tuple tuple : queryResult) {
				logger.error("Found a tuple which should not exist: {} / {}", i, tuple);
				System.exit(-1);
			}
		}
	}

	/**
	 * Insert new tuples
	 * @param bboxdbClient
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws BBoxDBException 
	 */
	private static void insertNewTuples(final BBoxDBCluster bboxdbClient) throws InterruptedException, ExecutionException, BBoxDBException {
		logger.info("Inserting new tuples");
		
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(i);
			final Tuple myTuple = new Tuple(key, new BoundingBox(1.0d, 2.0d, 1.0d, 2.0d), "test".getBytes());
			final EmptyResultFuture insertResult = bboxdbClient.insertTuple(TABLE, myTuple);
			insertResult.waitForAll();
			
			if(insertResult.isFailed()) {
				logger.error("Got an error during tuple insert: ", insertResult.getAllMessages());
				System.exit(-1);
			}
		}
	}
}

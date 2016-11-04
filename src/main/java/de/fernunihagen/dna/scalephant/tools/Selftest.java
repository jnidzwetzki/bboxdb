package de.fernunihagen.dna.scalephant.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import de.fernunihagen.dna.scalephant.network.client.OperationFuture;
import de.fernunihagen.dna.scalephant.network.client.ScalephantCluster;
import de.fernunihagen.dna.scalephant.network.client.ScalephantException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class Selftest {

	/**
	 * The name of the distribution group
	 */
	private static final String DISTRIBUTION_GROUP = "2_testgroup";
	
	/**
	 * The table to query
	 */
	private static final String TABLE = DISTRIBUTION_GROUP + "_mytable";
	
	/**
	 * The amount of operations
	 */
	private static final int NUMBER_OF_OPERATIONS = 10000;

	public static void main(final String[] args) throws InterruptedException, ExecutionException, ScalephantException {
		
		if(args.length < 2) {
			System.err.println("Usage: Selftest <Cluster-Name> <Cluster-Endpoint1> <Cluster-EndpointN>");
			System.exit(-1);
		}

		System.out.println("Running selftest......");
		
		final String clustername = args[1];
		final Collection<String> endpoints = new ArrayList<String>();
		for(int i = 1; i < args.length; i++) {
			endpoints.add(args[i]);
		}
	
		final ScalephantCluster scalephantClient = new ScalephantCluster(endpoints, clustername); 
		scalephantClient.connect();
		
		if(! scalephantClient.isConnected()) {
			System.err.println("Connection could not be established");
			System.exit(-1);
		}
		
		final OperationFuture deleteFuture = scalephantClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteFuture.waitForAll();
		if(deleteFuture.isFailed()) {
			System.err.println("Unable to delete distribution group: " + DISTRIBUTION_GROUP);
			System.exit(-1);
		}
		
		final OperationFuture createFuture = scalephantClient.createDistributionGroup(DISTRIBUTION_GROUP, (short) 2);
		createFuture.waitForAll();
		if(createFuture.isFailed()) {
			System.err.println("Unable to create distribution group: " + DISTRIBUTION_GROUP);
			System.exit(-1);
		}
		
		executeSelftest(scalephantClient);
	}

	/**
	 * Execute the selftest
	 * @param scalephantClient
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws ScalephantException 
	 */
	private static void executeSelftest(final ScalephantCluster scalephantClient) throws InterruptedException, ExecutionException, ScalephantException {
		final Random random = new Random();
		
		while(true) {
			insertNewTuples(scalephantClient);
			queryForTuples(scalephantClient, random);
			deleteTuples(scalephantClient);
			
			Thread.sleep(1000);
		}
	}

	/**
	 * Delete the stored tuples
	 * @param scalephantClient
	 * @throws InterruptedException
	 * @throws ScalephantException 
	 */
	private static void deleteTuples(final ScalephantCluster scalephantClient) throws InterruptedException, ScalephantException {
		System.out.println("Deleting tuples");
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(i);
			final OperationFuture deletionResult = scalephantClient.deleteTuple(TABLE, key);
			deletionResult.wait();
		}
	}

	/**
	 * Query for the stored tuples
	 * @param scalephantClient
	 * @param random
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws ScalephantException 
	 */
	private static void queryForTuples(final ScalephantCluster scalephantClient, final Random random) throws InterruptedException, ExecutionException, ScalephantException {
		System.out.println("Query for tuples");
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(random.nextInt() % NUMBER_OF_OPERATIONS);
			final OperationFuture queryResult = scalephantClient.queryKey(TABLE, key);
			queryResult.waitForAll();
			final Tuple result = (Tuple) queryResult.get(0);
			if(result.getKey() != key) {
				System.err.println("Got tuple with wrong key");
				System.exit(-1);
			}
		}
	}

	/**
	 * Insert new tuples
	 * @param scalephantClient
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws ScalephantException 
	 */
	private static void insertNewTuples(final ScalephantCluster scalephantClient) throws InterruptedException, ExecutionException, ScalephantException {
		System.out.println("Inserting new tuples");
		
		for(int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
			final String key = Integer.toString(i);
			final Tuple myTuple = new Tuple(key, new BoundingBox(1.0f, 2.0f, 1.0f, 2.0f), "test".getBytes());
			final OperationFuture insertResult = scalephantClient.insertTuple(TABLE, myTuple);
			insertResult.waitForAll();
			
			if(insertResult.isFailed()) {
				System.err.println("Got an error during tuple insert");
				System.exit(-1);
			}
		}
	}
}

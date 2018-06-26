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
package org.bboxdb.network;

import java.util.concurrent.ExecutionException;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBBoxDBCluster {

	/**
	 * The cluster contact point
	 */
	private static final String CLUSTER_CONTACT_POINT = "localhost:2181";

	/**
	 * The distribution group
	 */
	private static final String DISTRIBUTION_GROUP = "testgroup";
	
	/**
	 * The instance of the software
	 */
	private static BBoxDBMain bboxDBMain;
	
	
	@BeforeClass
	public static void init() throws Exception {
		bboxDBMain = new BBoxDBMain();
		bboxDBMain.init();
		bboxDBMain.start();
		
		// Allow connections to localhost (needed for travis CI test)
		MembershipConnectionService.getInstance().clearBlacklist();
		
		Thread.currentThread();
		// Wait some time to let the server process start
		Thread.sleep(5000);
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		if(bboxDBMain != null) {
			bboxDBMain.stop();
			bboxDBMain = null;
		}
		
		// Wait some time for socket re-use
		Thread.sleep(5000);
	}
	
	/**
	 * Re-create distribution group for each test
	 * @throws InterruptedException
	 * @throws BBoxDBException 
	 */
	@Before
	public void before() throws InterruptedException, BBoxDBException {
		final BBoxDB bboxdbClient = connectToServer();
		TestHelper.recreateDistributionGroup(bboxdbClient, DISTRIBUTION_GROUP);
		disconnect(bboxdbClient);
	}
	
	/**
	 * Integration test for the disconnect package
	 * @throws InterruptedException 
	 * 
	 */
	@Test(timeout=60000)
	public void testSendDisconnectPackage() throws InterruptedException {
		System.out.println("=== Running cluster testSendDisconnectPackage");

		final BBoxDBCluster bboxdbClient = connectToServer();
		Assert.assertTrue(bboxdbClient.isConnected());
		bboxdbClient.close();
		Assert.assertFalse(bboxdbClient.isConnected());
		
		System.out.println("=== End cluster testSendDisconnectPackage");
		disconnect(bboxdbClient);
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testInsertAndBoundingBoxTimeQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testInsertAndBoundingBoxTimeQuery");

		final BBoxDB bboxDBClient = connectToServer();

		NetworkQueryHelper.executeBoudingboxAndTimeQuery(bboxDBClient, DISTRIBUTION_GROUP);

		System.out.println("=== End cluster testInsertAndBoundingBoxTimeQuery");
		
		disconnect(bboxDBClient);
	}
	
	/**
	 * Build a new connection to the bboxdb server
	 * 
	 * @return
	 * @throws InterruptedException 
	 */
	protected BBoxDBCluster connectToServer() throws InterruptedException {
		final String clusterName = BBoxDBConfigurationManager.getConfiguration().getClustername();
		final BBoxDBCluster bboxdbCluster = new BBoxDBCluster(CLUSTER_CONTACT_POINT, clusterName);
	
		final boolean result = bboxdbCluster.connect();
		Assert.assertTrue(result);
		
		Thread.sleep(50);
		
		Assert.assertTrue(bboxdbCluster.isConnected());
		
		return bboxdbCluster;
	}
	
	/**
	 * The the insert and the deletion of a tuple
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testInsertAndDelete() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testInsertAndDelete");

		final BBoxDB bboxdbClient = connectToServer();

		NetworkQueryHelper.testInsertAndDeleteTuple(bboxdbClient, DISTRIBUTION_GROUP);
		System.out.println("=== End cluster testInsertAndDelete");
		
		disconnect(bboxdbClient);
	}
	
	/**
	 * Test the tuple join
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testJoin() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testJoin");

		final BBoxDB bboxdbClient = connectToServer();

		NetworkQueryHelper.executeJoinQuery(bboxdbClient, DISTRIBUTION_GROUP);
		
		System.out.println("=== End cluster testJoin");
		
		disconnect(bboxdbClient);
	}
	
	/**
	 * Execute the version time query
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testVersionTimeQuery() throws InterruptedException, BBoxDBException {
		final BBoxDB bboxDBClient = connectToServer();

		NetworkQueryHelper.testVersionTimeQuery(bboxDBClient, DISTRIBUTION_GROUP);
		disconnect(bboxDBClient);
	}
	
	/**
	 * Execute the version inserted query
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testInsertedTimeQuery() throws InterruptedException, BBoxDBException {
		final BBoxDB bboxDBClient = connectToServer();

		NetworkQueryHelper.testInsertedTimeQuery(bboxDBClient, DISTRIBUTION_GROUP);
		disconnect(bboxDBClient);
	}
	
	/**
	 * Disconnect from server
	 * @param bboxDBConnection
	 */
	protected void disconnect(final BBoxDB bboxDBClient) {
		bboxDBClient.close();
		Assert.assertFalse(bboxDBClient.isConnected());
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testInsertAndBoundingBoxContinousQuery() throws InterruptedException, 
	ExecutionException, BBoxDBException {
		
		final BBoxDB bboxDBClient = connectToServer();
		
		final String table = DISTRIBUTION_GROUP + "_relation9991";
		
		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());
		
		// Execute query
		final TupleListFuture result = bboxDBClient.queryRectangleContinuous(table, new Hyperrectangle(-1d, 2d, -1d, 2d));

		Assert.assertFalse(result.isFailed());
		
		disconnect(bboxDBClient);
	}
	
	/**
	 * Test misc methods
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testMiscMethods() throws InterruptedException {
		final BBoxDB bboxDBClient = connectToServer();
		Assert.assertTrue(bboxDBClient.toString().length() > 10);
		Assert.assertTrue(bboxDBClient.getTuplesPerPage() >= -1);
		bboxDBClient.isPagingEnabled();
		disconnect(bboxDBClient);
	}
}

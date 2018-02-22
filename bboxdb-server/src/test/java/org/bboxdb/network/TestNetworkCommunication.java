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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestNetworkCommunication {

	/**
	 * The instance of the software
	 */
	protected static BBoxDBMain bboxDBMain;
	
	/**
	 * The replication factor for the unit tests
	 */
	public final static short REPLICATION_FACTOR = 1;

	private static final String DISTRIBUTION_GROUP = "testgroup";
	
	@BeforeClass
	public static void init() throws Exception {
		bboxDBMain = new BBoxDBMain();
		bboxDBMain.init();
		bboxDBMain.start();
		
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
	 */
	@Before
	public void before() throws InterruptedException {
		
		// Delete distribution group
		final BBoxDBClient bboxDBClient = connectToServer();
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder.create(2)
				.withReplicationFactor((short) 1)
				.build();
		
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(DISTRIBUTION_GROUP, 
				configuration);
		
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
	}
	
	/**
	 * Integration test for the disconnect package
	 * 
	 */
	@Test
	public void testSendDisconnectPackage() {
		System.out.println("=== Running testSendDisconnectPackage");

		final BBoxDBClient bboxDBClient = connectToServer();
		Assert.assertTrue(bboxDBClient.isConnected());
		disconnectFromServer(bboxDBClient);
		Assert.assertFalse(bboxDBClient.isConnected());
		
		System.out.println("=== End testSendDisconnectPackage");
	}
	
	/**
	 * Send a delete package to the server
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@Test
	public void sendDeletePackage() throws InterruptedException, ExecutionException {
		System.out.println("=== Running sendDeletePackage");

		final BBoxDBClient bboxDBClient = connectToServer();
		
		final EmptyResultFuture result = bboxDBClient.deleteTable(DISTRIBUTION_GROUP + "_relation3");
		
		result.waitForAll();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertTrue(bboxDBClient.getConnectionState().isInRunningState());
		
		disconnectFromServer(bboxDBClient);
		Assert.assertFalse(bboxDBClient.isConnected());
		
		System.out.println("=== End sendDeletePackage");
	}
	
	/**
	 * Test the state machine of the connection
	 */
	@Test
	public void testConnectionState() {
		System.out.println("=== Running testConnectionState");

		final int port = BBoxDBConfigurationManager.getConfiguration().getNetworkListenPort();
		final BBoxDBClient bboxDBClient = new BBoxDBClient(new InetSocketAddress("127.0.0.1", port));
		Assert.assertTrue(bboxDBClient.getConnectionState().isInNewState());
		bboxDBClient.connect();
		Assert.assertTrue(bboxDBClient.getConnectionState().isInRunningState());
		bboxDBClient.disconnect();
		Assert.assertTrue(bboxDBClient.getConnectionState().isInTerminatedState());
	
		System.out.println("=== End testConnectionState");
	}
	
	/**
	 * Send a delete package to the server
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@Test
	public void sendDeletePackage2() throws InterruptedException, ExecutionException {
		System.out.println("=== Running sendDeletePackage2");
		
		final String table = DISTRIBUTION_GROUP + "_relation3";

		final BBoxDBClient bboxDBClient = connectToServer();
		
		// First call
		final EmptyResultFuture result1 = bboxDBClient.deleteTable(table);
		result1.waitForAll();
		Assert.assertTrue(result1.isDone());
		Assert.assertFalse(result1.isFailed());
		Assert.assertTrue(bboxDBClient.getConnectionState().isInRunningState());
		
		// Wait for command processing
		Thread.sleep(1000);
		
		// Second call
		final EmptyResultFuture result2 = bboxDBClient.deleteTable(table);
		result2.waitForAll();
		Assert.assertTrue(result2.isDone());
		Assert.assertFalse(result2.isFailed());
		Assert.assertTrue(bboxDBClient.getConnectionState().isInRunningState());
		
		disconnectFromServer(bboxDBClient);
		Assert.assertFalse(bboxDBClient.isConnected());		
		
		System.out.println("=== End sendDeletePackage2");
	}

	/**
	 * The the insert and the deletion of a tuple
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testInsertAndDelete() throws InterruptedException, ExecutionException, BBoxDBException {
		final BBoxDBClient bboxdbClient = connectToServer();

		NetworkQueryHelper.testInsertAndDeleteTuple(bboxdbClient);
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testInsertAndBoundingBoxQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		
		final BBoxDBClient bboxDBClient = connectToServer();

		NetworkQueryHelper.testBoundingBoxQuery(bboxDBClient);
	}
	
	/**
	 * Test the tuple join
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testJoin() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running network testJoin");

		final BBoxDB bboxdbClient = connectToServer();

		NetworkQueryHelper.executeJoinQuery(bboxdbClient);
		
		System.out.println("=== End network testJoin");
	}
	
	/**
	 * Insert some tuples and request it via paging
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testPaging() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running testPaging");
		final String table = DISTRIBUTION_GROUP + "_relation9999";
		
		final BBoxDBClient bboxDBClient = connectToServer();
		
		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForAll();
		Assert.assertFalse(resultCreateTable.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes());
		final EmptyResultFuture result1 = bboxDBClient.insertTuple(table, tuple1);
		
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes());
		final EmptyResultFuture result2 = bboxDBClient.insertTuple(table, tuple2);
		
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes());
		final EmptyResultFuture result3 = bboxDBClient.insertTuple(table, tuple3);		
		
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		final EmptyResultFuture result4 = bboxDBClient.insertTuple(table, tuple4);
		
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1d, 2d, 1d, 2d), "lmn".getBytes());
		final EmptyResultFuture result5 = bboxDBClient.insertTuple(table, tuple5);
		
		result1.waitForAll();
		result2.waitForAll();
		result3.waitForAll();
		result4.waitForAll();
		result5.waitForAll();

		// Without paging
		System.out.println("Pages = unlimited");
		bboxDBClient.setPagingEnabled(false);
		bboxDBClient.setTuplesPerPage((short) 0);
		final TupleListFuture future = bboxDBClient.queryBoundingBox(table, new BoundingBox(-10d, 10d, -10d, 10d));
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());		
		Assert.assertEquals(5, resultList.size());
		
		// With paging (tuples per page 10)
		System.out.println("Pages = 10");
		bboxDBClient.setPagingEnabled(true);
		bboxDBClient.setTuplesPerPage((short) 10);
		final TupleListFuture future2 = bboxDBClient.queryBoundingBox(table, new BoundingBox(-10d, 10d, -10d, 10d));
		future2.waitForAll();
		final List<Tuple> resultList2 = Lists.newArrayList(future2.iterator());		
		Assert.assertEquals(5, resultList2.size());
		
		// With paging (tuples per page 5)
		System.out.println("Pages = 5");
		bboxDBClient.setPagingEnabled(true);
		bboxDBClient.setTuplesPerPage((short) 5);
		final TupleListFuture future3 = bboxDBClient.queryBoundingBox(table, new BoundingBox(-10d, 10d, -10d, 10d));
		future3.waitForAll();
		final List<Tuple> resultList3 = Lists.newArrayList(future3.iterator());		
		Assert.assertEquals(5, resultList3.size());
		
		// With paging (tuples per page 2)
		System.out.println("Pages = 2");
		bboxDBClient.setPagingEnabled(true);
		bboxDBClient.setTuplesPerPage((short) 2);
		final TupleListFuture future4 = bboxDBClient.queryBoundingBox(table, new BoundingBox(-10d, 10d, -10d, 10d));
		System.out.println("Client is waiting on: " + future4);
		future4.waitForAll();
		final List<Tuple> resultList4 = Lists.newArrayList(future4.iterator());		
		Assert.assertEquals(5, resultList4.size());
		
		// With paging (tuples per page 1)
		System.out.println("Pages = 1");
		bboxDBClient.setPagingEnabled(true);
		bboxDBClient.setTuplesPerPage((short) 1);
		final TupleListFuture future5 = bboxDBClient.queryBoundingBox(table, new BoundingBox(-10d, 10d, -10d, 10d));
		future5.waitForAll();
		final List<Tuple> resultList5 = Lists.newArrayList(future5.iterator());		
		Assert.assertEquals(5, resultList5.size());
		
		System.out.println("=== End testPaging");
	}
	
	/**
	 * Insert a tuple and requet it via key
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testGetByKey() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running testGetByKey");
		final String table = DISTRIBUTION_GROUP + "_relation12333";
		
		final BBoxDBClient bboxDBClient = connectToServer();
		
		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForAll();
		Assert.assertFalse(resultCreateTable.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes());
		final EmptyResultFuture result1 = bboxDBClient.insertTuple(table, tuple1);
		
		result1.waitForAll();

		final TupleListFuture future = bboxDBClient.queryKey(table, "abc");
		future.waitForAll();
		
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());		
		Assert.assertEquals(1, resultList.size());
	
		System.out.println("=== End testGetByKey");
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testInsertAndBoundingBoxTimeQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running testInsertAndBoundingBoxTimeQuery");

		final BBoxDBClient bboxDBClient = connectToServer();

		NetworkQueryHelper.executeBoudingboxAndTimeQuery(bboxDBClient);

		System.out.println("=== End testInsertAndBoundingBoxTimeQuery");
	}
	
	/**
	 * Send a keep alive package to the server
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@Test
	public void sendKeepAlivePackage() throws InterruptedException, ExecutionException {
		System.out.println("=== Running sendKeepAlivePackage");

		final BBoxDBClient bboxDBClient = connectToServer();
		
		final EmptyResultFuture result = bboxDBClient.sendKeepAlivePackage();
		result.waitForAll();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertTrue(bboxDBClient.getConnectionState().isInRunningState());
		
		disconnectFromServer(bboxDBClient);
		Assert.assertFalse(bboxDBClient.isConnected());
		
		System.out.println("=== End sendKeepAlivePackage");
	}
	
	/**
	 * Build a new connection to the bboxdb server
	 * 
	 * @return
	 */
	protected BBoxDBClient connectToServer() {
		final int port = BBoxDBConfigurationManager.getConfiguration().getNetworkListenPort();
		final BBoxDBClient bboxDBClient = new BBoxDBClient(new InetSocketAddress("127.0.0.1", port));
		
		if(compressPackages()) {
			bboxDBClient.getClientCapabilities().setGZipCompression();
			Assert.assertTrue(bboxDBClient.getClientCapabilities().hasGZipCompression());
		} else {
			bboxDBClient.getClientCapabilities().clearGZipCompression();
			Assert.assertFalse(bboxDBClient.getClientCapabilities().hasGZipCompression());
		}
		
		Assert.assertFalse(bboxDBClient.isConnected());
		boolean result = bboxDBClient.connect();
		Assert.assertTrue(result);
		Assert.assertTrue(bboxDBClient.isConnected());
		
		if(compressPackages()) { 
			Assert.assertTrue(bboxDBClient.getConnectionCapabilities().hasGZipCompression());
		} else {
			Assert.assertFalse(bboxDBClient.getConnectionCapabilities().hasGZipCompression());
		}
		
		return bboxDBClient;
	}
	
	/**
	 * Should the packages be compressed or not
	 * @return
	 */
	protected boolean compressPackages() {
		return false;
	}
	
	/**
	 * Disconnect from server
	 * @param bboxDBClient
	 */
	protected void disconnectFromServer(final BBoxDBClient bboxDBClient) {
		bboxDBClient.disconnect();
		Assert.assertFalse(bboxDBClient.isConnected());
		Assert.assertEquals(0, bboxDBClient.getInFlightCalls());
	}
}

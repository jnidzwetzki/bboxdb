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
package org.bboxdb.network;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.junit.AfterClass;
import org.junit.Assert;
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
		
		final EmptyResultFuture result = bboxDBClient.deleteTable("1_testgroup1_relation3");
		
		result.waitForAll();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, bboxDBClient.getConnectionState());
		
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
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_CLOSED, bboxDBClient.getConnectionState());
		bboxDBClient.connect();
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, bboxDBClient.getConnectionState());
		bboxDBClient.disconnect();
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_CLOSED, bboxDBClient.getConnectionState());
	
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

		final BBoxDBClient bboxDBClient = connectToServer();
		
		// First call
		final EmptyResultFuture result1 = bboxDBClient.deleteTable("1_testgroup1_relation3");
		result1.waitForAll();
		Assert.assertTrue(result1.isDone());
		Assert.assertFalse(result1.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, bboxDBClient.getConnectionState());
		
		// Wait for command processing
		Thread.sleep(1000);
		
		// Second call
		final EmptyResultFuture result2 = bboxDBClient.deleteTable("1_testgroup1_relation3");
		result2.waitForAll();
		Assert.assertTrue(result2.isDone());
		Assert.assertFalse(result2.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, bboxDBClient.getConnectionState());
		
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
		System.out.println("=== Running testInsertAndDelete");

		final String distributionGroup = "1_testgroupdel"; 
		final String table = distributionGroup + "_relation4";
		final String key = "key12";
		
		final BBoxDBClient bboxdbClient = connectToServer();
		
		// Delete distribution group
		System.out.println("Delete distribution group");
		final EmptyResultFuture resultDelete = bboxdbClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		System.out.println("Create distribution group");
		final EmptyResultFuture resultCreate = bboxdbClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		System.out.println("Delete tuple");
		final EmptyResultFuture deleteResult1 = bboxdbClient.deleteTuple(table, key);
		deleteResult1.waitForAll();
		Assert.assertFalse(deleteResult1.isFailed());
		Assert.assertTrue(deleteResult1.isDone());
		
		System.out.println("Query key");
		final TupleListFuture getResult = bboxdbClient.queryKey(table, key);
		getResult.waitForAll();
		Assert.assertFalse(getResult.isFailed());
		Assert.assertTrue(getResult.isDone());
		
		System.out.println("Insert tuple");
		final Tuple tuple = new Tuple(key, BoundingBox.EMPTY_BOX, "abc".getBytes());
		final EmptyResultFuture insertResult = bboxdbClient.insertTuple(table, tuple);
		insertResult.waitForAll();
		Assert.assertFalse(insertResult.isFailed());
		Assert.assertTrue(insertResult.isDone());

		System.out.println("Query key 2");
		final TupleListFuture getResult2 = bboxdbClient.queryKey(table, key);
		getResult2.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(getResult2.iterator());
		Assert.assertEquals(tuple, resultList.get(0));

		System.out.println("Delete tuple 2");
		final EmptyResultFuture deleteResult2 = bboxdbClient.deleteTuple(table, key, System.currentTimeMillis());
		deleteResult2.waitForAll();
		Assert.assertFalse(deleteResult2.isFailed());
		Assert.assertTrue(deleteResult2.isDone());
		
		System.out.println("Query key 3");
		final TupleListFuture getResult3 = bboxdbClient.queryKey(table, key);
		getResult3.waitForAll();
		Assert.assertFalse(getResult3.isFailed());
		Assert.assertTrue(getResult3.isDone());
		
		// Disconnect
		disconnectFromServer(bboxdbClient);
		
		System.out.println("=== End testInsertAndDelete");
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testInsertAndBoundingBoxQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running testInsertAndBoundingBoxQuery");
		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		final BBoxDBClient bboxDBClient = connectToServer();
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes());
		bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes());
		bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes());
		bboxDBClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		bboxDBClient.insertTuple(table, tuple5);

		final TupleListFuture future = bboxDBClient.queryBoundingBox(table, new BoundingBox(-1d, 2d, -1d, 2d));
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());
		
		Assert.assertEquals(3, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
		System.out.println("=== End testInsertAndBoundingBoxQuery");
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
		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		final BBoxDBClient bboxDBClient = connectToServer();
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes());
		bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes());
		bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes());
		bboxDBClient.insertTuple(table, tuple3);		
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1d, 2d, 1d, 2d), "lmn".getBytes());
		bboxDBClient.insertTuple(table, tuple5);

		// Without paging
		bboxDBClient.setPagingEnabled(false);
		bboxDBClient.setTuplesPerPage((short) 0);
		final TupleListFuture future = bboxDBClient.queryBoundingBox(table, new BoundingBox(-10d, 10d, -10d, 10d));
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());		
		Assert.assertEquals(5, resultList.size());
		
		// With paging (tuples per page 10)
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
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testInsertAndBoundingBoxTimeQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running testInsertAndBoundingBoxTimeQuery");

		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		final BBoxDBClient bboxDBClient = connectToServer();
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes(), 4);
		bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes(), 4);
		bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes(), 1);
		bboxDBClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		bboxDBClient.insertTuple(table, tuple5);

		final TupleListFuture future = bboxDBClient.queryBoundingBoxAndTime(table, new BoundingBox(-1d, 2d, -1d, 2d), 2);
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertFalse(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
		
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
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, bboxDBClient.getConnectionState());
		
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

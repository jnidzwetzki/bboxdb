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
package org.bboxdb.network;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.daemon.DaemonInitException;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.BBoxDBMain;
import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.IteratorHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNetworkCommunication {

	/**
	 * The instance of the software
	 */
	protected static BBoxDBMain scalephantMain;
	
	/**
	 * The replication factor for the unit tests
	 */
	public final static short REPLICATION_FACTOR = 1;
	
	@BeforeClass
	public static void init() throws DaemonInitException, Exception {
		scalephantMain = new BBoxDBMain();
		scalephantMain.init(null);
		scalephantMain.start();
		
		Thread.currentThread();
		// Wait some time to let the server process start
		Thread.sleep(5000);
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		if(scalephantMain != null) {
			scalephantMain.stop();
			scalephantMain.destroy();
			scalephantMain = null;
		}
		
		// Wait some time for socket resuse
		Thread.sleep(5000);
	}
	
	/**
	 * Integration test for the disconnect package
	 * 
	 */
	@Test
	public void testSendDisconnectPackage() {
		final BBoxDBClient scalephantClient = connectToServer();
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());
	}
	
	/**
	 * Send a delete package to the server
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@Test
	public void sendDeletePackage() throws InterruptedException, ExecutionException {
		final BBoxDBClient scalephantClient = connectToServer();
		
		final EmptyResultFuture result = scalephantClient.deleteTable("1_testgroup1_relation3");
		
		result.waitForAll();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());
	}
	
	/**
	 * Test the state machine of the connection
	 */
	@Test
	public void testConnectionState() {
		final int port = BBoxDBConfigurationManager.getConfiguration().getNetworkListenPort();
		final BBoxDBClient scalephantClient = new BBoxDBClient("127.0.0.1", port);
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_CLOSED, scalephantClient.getConnectionState());
		scalephantClient.connect();
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		scalephantClient.disconnect();
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_CLOSED, scalephantClient.getConnectionState());
	}
	
	/**
	 * Send a delete package to the server
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@Test
	public void sendDeletePackage2() throws InterruptedException, ExecutionException {
		final BBoxDBClient scalephantClient = connectToServer();
		
		// First call
		final EmptyResultFuture result1 = scalephantClient.deleteTable("1_testgroup1_relation3");
		result1.waitForAll();
		Assert.assertTrue(result1.isDone());
		Assert.assertFalse(result1.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		// Wait for command processing
		Thread.sleep(1000);
		
		// Second call
		final EmptyResultFuture result2 = scalephantClient.deleteTable("1_testgroup1_relation3");
		result2.waitForAll();
		Assert.assertTrue(result2.isDone());
		Assert.assertFalse(result2.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());		
	}

	/**
	 * The the insert and the deletion of a tuple
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testInsertAndDelete() throws InterruptedException, ExecutionException {
		final String distributionGroup = "1_testgroup1"; 
		final String table = distributionGroup + "_relation4";
		final String key = "key12";
		
		final BBoxDBClient scalephantClient = connectToServer();
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = scalephantClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = scalephantClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		final EmptyResultFuture deleteResult1 = scalephantClient.deleteTuple(table, key, System.currentTimeMillis());
		deleteResult1.waitForAll();
		Assert.assertFalse(deleteResult1.isFailed());
		Assert.assertTrue(deleteResult1.isDone());
		
		final TupleListFuture getResult = scalephantClient.queryKey(table, key);
		getResult.waitForAll();
		Assert.assertFalse(getResult.isFailed());
		Assert.assertTrue(getResult.isDone());
		
		final Tuple tuple = new Tuple(key, BoundingBox.EMPTY_BOX, "abc".getBytes());
		final EmptyResultFuture insertResult = scalephantClient.insertTuple(table, tuple);
		insertResult.waitForAll();
		Assert.assertFalse(insertResult.isFailed());
		Assert.assertTrue(insertResult.isDone());

		final TupleListFuture getResult2 = scalephantClient.queryKey(table, key);
		getResult2.waitForAll();
		final List<Tuple> resultList = IteratorHelper.iteratorToList(getResult2.iterator());
		Assert.assertEquals(tuple, resultList.get(0));

		final EmptyResultFuture deleteResult2 = scalephantClient.deleteTuple(table, key, System.currentTimeMillis());
		deleteResult2.waitForAll();
		Assert.assertFalse(deleteResult2.isFailed());
		Assert.assertTrue(deleteResult2.isDone());
		
		final TupleListFuture getResult3 = scalephantClient.queryKey(table, key);
		getResult3.waitForAll();
		Assert.assertFalse(getResult3.isFailed());
		Assert.assertTrue(getResult3.isDone());
		
		// Disconnect
		disconnectFromServer(scalephantClient);
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testInsertAndBoundingBoxQuery() throws InterruptedException, ExecutionException {
		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		final BBoxDBClient scalephantClient = connectToServer();
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = scalephantClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = scalephantClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0f, 1f, 0f, 1f), "abc".getBytes());
		scalephantClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0f, 0.5f, 0f, 0.5f), "def".getBytes());
		scalephantClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5f, 1.5f, 0.5f, 1.5f), "geh".getBytes());
		scalephantClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10f, -9f, -10f, -9f), "ijk".getBytes());
		scalephantClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000f, 1001f, 1000f, 1001f), "lmn".getBytes());
		scalephantClient.insertTuple(table, tuple5);

		final TupleListFuture future = scalephantClient.queryBoundingBox(table, new BoundingBox(-1f, 2f, -1f, 2f));
		future.waitForAll();
		final List<Tuple> resultList = IteratorHelper.iteratorToList(future.iterator());
		
		Assert.assertEquals(3, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testInsertAndBoundingBoxTimeQuery() throws InterruptedException, ExecutionException {
		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		final BBoxDBClient scalephantClient = connectToServer();
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = scalephantClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = scalephantClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0f, 1f, 0f, 1f), "abc".getBytes(), 4);
		scalephantClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0f, 0.5f, 0f, 0.5f), "def".getBytes(), 4);
		scalephantClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5f, 1.5f, 0.5f, 1.5f), "geh".getBytes(), 1);
		scalephantClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10f, -9f, -10f, -9f), "ijk".getBytes());
		scalephantClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000f, 1001f, 1000f, 1001f), "lmn".getBytes());
		scalephantClient.insertTuple(table, tuple5);

		final TupleListFuture future = scalephantClient.queryBoundingBoxAndTime(table, new BoundingBox(-1f, 2f, -1f, 2f), 2);
		future.waitForAll();
		final List<Tuple> resultList = IteratorHelper.iteratorToList(future.iterator());
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertFalse(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
	}
	
	
	/**
	 * Send a keep alive package to the server
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@Test
	public void sendKeepAlivePackage() throws InterruptedException, ExecutionException {
		final BBoxDBClient scalephantClient = connectToServer();
		
		final EmptyResultFuture result = scalephantClient.sendKeepAlivePackage();
		result.waitForAll();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());
	}
	
	
	/**
	 * Build a new connection to the scalephant server
	 * 
	 * @return
	 */
	protected BBoxDBClient connectToServer() {
		final int port = BBoxDBConfigurationManager.getConfiguration().getNetworkListenPort();
		final BBoxDBClient scalephantClient = new BBoxDBClient("127.0.0.1", port);
		
		if(compressPackages()) {
			scalephantClient.getClientCapabilities().setGZipCompression();
			Assert.assertTrue(scalephantClient.getClientCapabilities().hasGZipCompression());
		} else {
			scalephantClient.getClientCapabilities().clearGZipCompression();
			Assert.assertFalse(scalephantClient.getClientCapabilities().hasGZipCompression());
		}
		
		Assert.assertFalse(scalephantClient.isConnected());
		boolean result = scalephantClient.connect();
		Assert.assertTrue(result);
		Assert.assertTrue(scalephantClient.isConnected());
		
		if(compressPackages()) { 
			Assert.assertTrue(scalephantClient.getConnectionCapabilities().hasGZipCompression());
		} else {
			Assert.assertFalse(scalephantClient.getConnectionCapabilities().hasGZipCompression());
		}
		
		return scalephantClient;
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
	 * @param scalephantClient
	 */
	protected void disconnectFromServer(final BBoxDBClient scalephantClient) {
		scalephantClient.disconnect();
		Assert.assertFalse(scalephantClient.isConnected());
		Assert.assertEquals(0, scalephantClient.getInFlightCalls());
	}
}

package de.fernunihagen.dna.jkn.scalephant.network;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.daemon.DaemonInitException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.ScalephantMain;
import de.fernunihagen.dna.jkn.scalephant.network.client.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class TestNetworkCommunication {

	protected static ScalephantMain scalephantMain;
	
	@BeforeClass
	public static void init() throws DaemonInitException, Exception {
		scalephantMain = new ScalephantMain();
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
	}
	
	/**
	 * Integration test for the disconnect package
	 * 
	 */
	@Test
	public void testSendDisconnectPackage() {
		final ScalephantClient scalephantClient = connectToServer();
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
		final ScalephantClient scalephantClient = connectToServer();
		
		ClientOperationFuture result = scalephantClient.deleteTable("1_testgroup1_relation3");
		
		result.waitForAll();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertTrue((Boolean) result.get(0));
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());
	}
	
	/**
	 * Test the state machine of the connection
	 */
	@Test
	public void testConnectionState() {
		final int port = ScalephantConfigurationManager.getConfiguration().getNetworkListenPort();
		final ScalephantClient scalephantClient = new ScalephantClient("127.0.0.1", port);
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
		final ScalephantClient scalephantClient = connectToServer();
		
		// First call
		ClientOperationFuture result1 = scalephantClient.deleteTable("1_testgroup1_relation3");
		result1.waitForAll();
		Assert.assertTrue(result1.isDone());
		Assert.assertFalse(result1.isFailed());
		Assert.assertTrue((Boolean) result1.get(0));
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		// Wait for command processing
		Thread.sleep(1000);
		
		// Second call
		ClientOperationFuture result2 = scalephantClient.deleteTable("1_testgroup1_relation3");
		result2.waitForAll();
		Assert.assertTrue(result2.isDone());
		Assert.assertFalse(result2.isFailed());
		Assert.assertTrue((Boolean) result2.get(0));
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
		
		final ScalephantClient scalephantClient = connectToServer();
		
		// Delete distribution group
		final ClientOperationFuture resultDelete = scalephantClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final ClientOperationFuture resultCreate = scalephantClient.createDistributionGroup(distributionGroup, (short) 3);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		final ClientOperationFuture deleteResult1 = scalephantClient.deleteTuple(table, key);
		final Object deleteResult1Object = deleteResult1.get(0);
		Assert.assertTrue(deleteResult1Object instanceof Boolean);
		Assert.assertTrue(((Boolean) deleteResult1Object).booleanValue());
		
		final ClientOperationFuture getResult = scalephantClient.queryKey(table, key);
		final Object getResultObject = getResult.get(0);
		Assert.assertTrue(getResultObject instanceof Boolean);
		Assert.assertTrue(((Boolean) getResultObject).booleanValue());
		
		final Tuple tuple = new Tuple(key, BoundingBox.EMPTY_BOX, "abc".getBytes());
		final ClientOperationFuture insertResult = scalephantClient.insertTuple(table, tuple);
		final Object insertResultObject = insertResult.get(0);
		Assert.assertTrue(insertResultObject instanceof Boolean);
		Assert.assertTrue(((Boolean) insertResultObject).booleanValue());

		final ClientOperationFuture getResult2 = scalephantClient.queryKey(table, key);
		final Object getResult2Object = getResult2.get(0);
		Assert.assertTrue(getResult2Object instanceof Tuple);
		final Tuple resultTuple = (Tuple) getResult2Object;
		Assert.assertEquals(tuple, resultTuple);

		final ClientOperationFuture deleteResult2 = scalephantClient.deleteTuple(table, key);
		final Object deleteResult2Object = deleteResult2.get(0);
		Assert.assertTrue(deleteResult2Object instanceof Boolean);
		Assert.assertTrue(((Boolean) deleteResult2Object).booleanValue());
		
		final ClientOperationFuture getResult3 = scalephantClient.queryKey(table, key);
		final Object getResult3Object = getResult3.get(0);
		Assert.assertTrue(getResult3Object instanceof Boolean);
		Assert.assertTrue(((Boolean) getResult3Object).booleanValue());
		
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
		final String table = distributionGroup + "_relation5";
		
		final ScalephantClient scalephantClient = connectToServer();
		
		// Delete distribution group
		final ClientOperationFuture resultDelete = scalephantClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final ClientOperationFuture resultCreate = scalephantClient.createDistributionGroup(distributionGroup, (short) 3);
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

		final ClientOperationFuture future = scalephantClient.queryBoundingBox(table, new BoundingBox(-1f, 2f, -1f, 2f));
		final Object result = future.get(0);
		
		Assert.assertTrue(result instanceof List);
		@SuppressWarnings("unchecked")
		final List<Tuple> resultList = (List<Tuple>) result;
		
		Assert.assertEquals(3, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
	}
	
	/**
	 * Build a new connection to the scalephant server
	 * 
	 * @return
	 */
	protected ScalephantClient connectToServer() {
		final int port = ScalephantConfigurationManager.getConfiguration().getNetworkListenPort();
		final ScalephantClient scalephantClient = new ScalephantClient("127.0.0.1", port);
		Assert.assertFalse(scalephantClient.isConnected());
		boolean result = scalephantClient.connect();
		Assert.assertTrue(result);
		Assert.assertTrue(scalephantClient.isConnected());
		return scalephantClient;
	}
	
	/**
	 * Disconnect from server
	 * @param scalephantClient
	 */
	protected void disconnectFromServer(final ScalephantClient scalephantClient) {
		scalephantClient.disconnect();
		Assert.assertFalse(scalephantClient.isConnected());
		Assert.assertEquals(0, scalephantClient.getInFlightCalls());
	}
}

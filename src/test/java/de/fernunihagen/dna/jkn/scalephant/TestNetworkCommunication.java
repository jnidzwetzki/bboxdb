package de.fernunihagen.dna.jkn.scalephant;

import java.util.concurrent.ExecutionException;

import junit.framework.Assert;

import org.apache.commons.daemon.DaemonInitException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.network.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

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
		
		ClientOperationFuture result = scalephantClient.deleteTable("testrelation");
		
		result.get();
		
		Assert.assertTrue(result.isDone());
		Assert.assertFalse(result.isFailed());
		Assert.assertTrue((Boolean) result.get());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());
	}
	
	/**
	 * Test the state machine of the connection
	 */
	@Test
	public void testConnectionState() {
		final ScalephantClient scalephantClient = new ScalephantClient("127.0.0.1");
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
		ClientOperationFuture result1 = scalephantClient.deleteTable("testrelation");
		result1.get();
		Assert.assertTrue(result1.isDone());
		Assert.assertFalse(result1.isFailed());
		Assert.assertTrue((Boolean) result1.get());
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		// Wait for command processing
		Thread.sleep(1000);
		
		// Second call
		ClientOperationFuture result2 = scalephantClient.deleteTable("testrelation");
		result2.get();
		Assert.assertTrue(result2.isDone());
		Assert.assertFalse(result2.isFailed());
		Assert.assertTrue((Boolean) result2.get());
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
		final String table = "testtable47";
		final String key = "key12";
		
		final ScalephantClient scalephantClient = connectToServer();
		
		final ClientOperationFuture deleteResult1 = scalephantClient.deleteTuple(table, key);
		final Object deleteResult1Object = deleteResult1.get();
		Assert.assertTrue(deleteResult1Object instanceof Boolean);
		Assert.assertTrue(((Boolean) deleteResult1Object).booleanValue());
		
		final ClientOperationFuture getResult = scalephantClient.queryKey(table, key);
		final Object getResultObject = getResult.get();
		Assert.assertTrue(getResultObject instanceof Boolean);
		Assert.assertTrue(((Boolean) getResultObject).booleanValue());
		
		final Tuple tuple = new Tuple(key, BoundingBox.EMPTY_BOX, "abc".getBytes());
		final ClientOperationFuture insertResult = scalephantClient.insertTuple(table, tuple);
		final Object insertResultObject = insertResult.get();
		Assert.assertTrue(insertResultObject instanceof Boolean);
		Assert.assertTrue(((Boolean) insertResultObject).booleanValue());

		final ClientOperationFuture getResult2 = scalephantClient.queryKey(table, key);
		final Object getResult2Object = getResult2.get();
		Assert.assertTrue(getResult2Object instanceof Tuple);
		final Tuple resultTuple = (Tuple) getResult2Object;
		Assert.assertEquals(tuple, resultTuple);

		final ClientOperationFuture deleteResult2 = scalephantClient.deleteTuple(table, key);
		final Object deleteResult2Object = deleteResult2.get();
		Assert.assertTrue(deleteResult2Object instanceof Boolean);
		Assert.assertTrue(((Boolean) deleteResult2Object).booleanValue());
		
		final ClientOperationFuture getResult3 = scalephantClient.queryKey(table, key);
		final Object getResult3Object = getResult3.get();
		Assert.assertTrue(getResult3Object instanceof Boolean);
		Assert.assertTrue(((Boolean) getResult3Object).booleanValue());
		
	}
	
	/**
	 * Build a new connection to the scalephant server
	 * 
	 * @return
	 */
	protected ScalephantClient connectToServer() {
		final ScalephantClient scalephantClient = new ScalephantClient("127.0.0.1");
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
	}
}

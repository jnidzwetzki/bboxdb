package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.apache.commons.daemon.DaemonInitException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantClient;

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
	 */
	@Test
	public void sendDeletePackage() {
		final ScalephantClient scalephantClient = connectToServer();
		
		boolean result = scalephantClient.deleteTable("testrelation");
		Assert.assertTrue(result);
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
	 */
	@Test
	public void sendDeletePackage2() throws InterruptedException {
		final ScalephantClient scalephantClient = connectToServer();
		
		// First call
		boolean result1 = scalephantClient.deleteTable("testrelation");
		Assert.assertTrue(result1);
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		// Wait for command processing
		Thread.sleep(1000);
		
		// Second call
		boolean result2 = scalephantClient.deleteTable("testrelation");
		Assert.assertTrue(result2);
		Assert.assertEquals(NetworkConnectionState.NETWORK_CONNECTION_OPEN, scalephantClient.getConnectionState());
		
		disconnectFromServer(scalephantClient);
		Assert.assertFalse(scalephantClient.isConnected());
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

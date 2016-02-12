package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.apache.commons.daemon.DaemonInitException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
	 * Send a truncate package to the server
	 */
	@Test
	public void sendDeletePackate() {
		final ScalephantClient scalephantClient = connectToServer();
		
		scalephantClient.deleteTable("testrelation");
		
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

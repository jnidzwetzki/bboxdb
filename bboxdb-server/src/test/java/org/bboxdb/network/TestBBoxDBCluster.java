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
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBBoxDBCluster {

	/**
	 * The instance of the software
	 */
	protected static BBoxDBMain bboxDBMain;
	
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
	 * Integration test for the disconnect package
	 * @throws InterruptedException 
	 * 
	 */
	@Test
	public void testSendDisconnectPackage() throws InterruptedException {
		System.out.println("=== Running cluster testSendDisconnectPackage");

		final BBoxDBCluster bboxdbClient = connectToServer();
		Assert.assertTrue(bboxdbClient.isConnected());
		bboxdbClient.disconnect();
		Assert.assertFalse(bboxdbClient.isConnected());
		
		System.out.println("=== End cluster testSendDisconnectPackage");
	}
	
	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testInsertAndBoundingBoxTimeQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testInsertAndBoundingBoxTimeQuery");

		final BBoxDB bboxDBClient = connectToServer();

		NetworkQueryHelper.executeBoudingboxAndTimeQuery(bboxDBClient);

		System.out.println("=== End cluster testInsertAndBoundingBoxTimeQuery");
	}
	
	/**
	 * Build a new connection to the bboxdb server
	 * 
	 * @return
	 * @throws InterruptedException 
	 */
	protected BBoxDBCluster connectToServer() throws InterruptedException {
		final String clusterName = BBoxDBConfigurationManager.getConfiguration().getClustername();
		final BBoxDBCluster bboxdbCluster = new BBoxDBCluster("localhost:2181", clusterName);
	
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
	@Test
	public void testInsertAndDelete() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testInsertAndDelete");

		final BBoxDB bboxdbClient = connectToServer();

		NetworkQueryHelper.testInsertAndDeleteTuple(bboxdbClient);
		System.out.println("=== End cluster testInsertAndDelete");

	}
	
	
	/**
	 * Test the tuple join
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testJoin() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testJoin");

		final BBoxDB bboxdbClient = connectToServer();

		NetworkQueryHelper.executeJoinQuery(bboxdbClient);
		
		System.out.println("=== End cluster testJoin");
	}
}

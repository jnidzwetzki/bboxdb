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
package org.bboxdb.networkproxy.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.TestHelper;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.networkproxy.ProxyConst;
import org.bboxdb.networkproxy.ProxyMain;
import org.bboxdb.networkproxy.client.NetworkProxyClient;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.util.EnvironmentHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProxyTest {

	/**
	 * The BBoxDB Main
	 */
	private static BBoxDBMain bboxDBMain;

	/**
	 * The proxy class
	 */
	private ProxyMain proxyMain;
	
	/**
	 * The name of the testgroup
	 */
	private static final String TEST_GROUP = "proxytestgroup";

	/**
	 * Test name of the first testtable
	 */
	private static final String TEST_TABLE_1 = TEST_GROUP + "_testtable1";
	
	/**
	 * Test name of the second testtable
	 */
	private static final String TEST_TABLE_2 = TEST_GROUP + "_testtable2";


	@BeforeClass
	public static void init() throws Exception {

		// Cleanup Zookeeper
		EnvironmentHelper.resetTestEnvironment();

		bboxDBMain = new BBoxDBMain();
		bboxDBMain.init();
		bboxDBMain.start();

		// Allow connections to localhost (needed for travis CI test)
		MembershipConnectionService.getInstance().clearBlacklist();

		Thread.currentThread();

		// Wait some time to let the server process start
		Thread.sleep(5000);
		
		// Create distribution group and tables
		final int port = BBoxDBConfigurationManager.getConfiguration().getNetworkListenPort();
		final BBoxDBConnection bboxDBconnection = new BBoxDBConnection(new InetSocketAddress("127.0.0.1", port));
		final BBoxDB client = bboxDBconnection.getBboxDBClient();
		
		TestHelper.recreateDistributionGroup(client, TEST_GROUP);
		
		final EmptyResultFuture resultCreateTable1 = client.createTable(TEST_TABLE_1, new TupleStoreConfiguration());
		resultCreateTable1.waitForCompletion();
		Assert.assertFalse(resultCreateTable1.isFailed());

		final EmptyResultFuture resultCreateTable2 = client.createTable(TEST_TABLE_2, new TupleStoreConfiguration());
		resultCreateTable2.waitForCompletion();
		Assert.assertFalse(resultCreateTable2.isFailed());
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

	@Before
	public synchronized void startProxyServer() {

		if(proxyMain != null) {
			throw new IllegalStateException("Proxy is already running");
		}

		proxyMain = new ProxyMain("127.0.0.1", "mycluster");
		proxyMain.run();
	}

	@After
	public synchronized void stopProxyServer() {
		proxyMain.close();
		proxyMain = null;
	}

	@Test(timeout=60000)
	public void testDisconnect() throws UnknownHostException, IOException {
		final NetworkProxyClient networkProxyClient = new NetworkProxyClient("localhost", ProxyConst.PROXY_PORT);
		networkProxyClient.disconnect();
		networkProxyClient.close();
	}
	
	@Test(timeout=60000)
	public void testPutAndGet() throws UnknownHostException, IOException {
		final NetworkProxyClient networkProxyClient = new NetworkProxyClient("localhost", ProxyConst.PROXY_PORT);
		
		final Tuple tuple = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abcd".getBytes());
		networkProxyClient.put(tuple, TEST_TABLE_1);
		
		networkProxyClient.disconnect();
		networkProxyClient.close();
	}
}

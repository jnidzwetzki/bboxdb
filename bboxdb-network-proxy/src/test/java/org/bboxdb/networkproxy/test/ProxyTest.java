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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.networkproxy.ProxyConst;
import org.bboxdb.networkproxy.ProxyMain;
import org.bboxdb.networkproxy.client.NetworkProxyClient;
import org.bboxdb.storage.entity.JoinedTuple;
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
	 * The proxy client
	 */
	private NetworkProxyClient networkProxyClient;

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

	/**
	 * The name of the first key
	 */
	private static final String KEY1 = "key1";

	/**
	 * The name of the second key
	 */
	private static final String KEY2 = "key2";

	/**
	 * The name of the third key
	 */
	private static final String KEY3 = "key3";


	@BeforeClass
	public static void init() throws Exception {

		// Cleanup Zookeeper
		EnvironmentHelper.resetTestEnvironment();

		bboxDBMain = new BBoxDBMain();
		bboxDBMain.init();
		bboxDBMain.start();

		// Allow connections to localhost (needed for travis CI test)
		MembershipConnectionService.getInstance().clearBlacklist();

		// Wait some time to let the server process start
		Thread.sleep(5000);

		// Create distribution group and tables
		System.out.println("==> Connect to server");
		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();

		System.out.println("==> Create distribution group");
		EnvironmentHelper.recreateDistributionGroup(bboxDBClient, TEST_GROUP);

		System.out.println("===> Create new table1");
		final EmptyResultFuture resultCreateTable1 = bboxDBClient.createTable(TEST_TABLE_1, new TupleStoreConfiguration());
		resultCreateTable1.waitForCompletion();
		Assert.assertFalse(resultCreateTable1.isFailed());

		System.out.println("===> Create new table2");
		final EmptyResultFuture resultCreateTable2 = bboxDBClient.createTable(TEST_TABLE_2, new TupleStoreConfiguration());
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
	public synchronized void startProxyServer() throws Exception {
		if(proxyMain != null) {
			throw new IllegalStateException("Proxy is already running");
		}

		proxyMain = new ProxyMain("127.0.0.1", "mycluster");
		proxyMain.run();

		if(networkProxyClient != null) {
			throw new IllegalStateException("Client is already active");
		}

		proxyMain.getServiceState().awaitRunning();

		networkProxyClient = new NetworkProxyClient("localhost", ProxyConst.PROXY_PORT);
	}

	@After
	public synchronized void stopProxyServer() throws IOException {
		// Close proxy connection
		if(networkProxyClient != null) {
			networkProxyClient.disconnect();
			networkProxyClient.close();
			networkProxyClient = null;
		}

		// Shutdown proxy
		if(proxyMain != null) {
			proxyMain.close();
			proxyMain = null;
		}
	}

	@Test(timeout=60_000)
	public void testDisconnect() throws UnknownHostException, IOException {
		Assert.assertTrue(networkProxyClient.isConnected());

		networkProxyClient.disconnect();
		networkProxyClient.close();
		Assert.assertFalse(networkProxyClient.isConnected());

		networkProxyClient = null;
	}

	@Test(timeout=60_000)
	public void testPutAndGet1() throws UnknownHostException, IOException {
		final List<Tuple> result1 = networkProxyClient.get(KEY1, TEST_TABLE_1);
		Assert.assertTrue(result1.isEmpty());

		final Tuple tuple = new Tuple(KEY1, Hyperrectangle.FULL_SPACE, "abcd".getBytes());
		networkProxyClient.put(tuple, TEST_TABLE_1);

		final List<Tuple> result2 = networkProxyClient.get(KEY1, TEST_TABLE_1);
		Assert.assertEquals(1, result2.size());
		Assert.assertEquals(tuple, result2.get(0));
	}

	@Test(timeout=60_000)
	public void testPutAndGet2() throws UnknownHostException, IOException {
		final Tuple tuple = new Tuple(KEY1, Hyperrectangle.FULL_SPACE, "abcd".getBytes());
		networkProxyClient.put(tuple, TEST_TABLE_1);

		final Tuple tuple2 = new Tuple(KEY1, Hyperrectangle.FULL_SPACE, KEY2.getBytes());
		networkProxyClient.put(tuple2, TEST_TABLE_1);

		final List<Tuple> result2 = networkProxyClient.get(KEY1, TEST_TABLE_1);
		Assert.assertEquals(1, result2.size());
		Assert.assertEquals(tuple2, result2.get(0));
	}

	@Test(timeout=60_000)
	public void testPutAndDelete() throws UnknownHostException, IOException {
		final Tuple tuple = new Tuple(KEY1, Hyperrectangle.FULL_SPACE, "abcd".getBytes());
		networkProxyClient.put(tuple, TEST_TABLE_1);

		final List<Tuple> result1 = networkProxyClient.get(KEY1, TEST_TABLE_1);
		Assert.assertEquals(1, result1.size());
		Assert.assertEquals(tuple, result1.get(0));

		networkProxyClient.delete(KEY1, TEST_TABLE_1);

		final List<Tuple> result2 = networkProxyClient.get(KEY1, TEST_TABLE_1);
		Assert.assertTrue(result2.isEmpty());
	}

	@Test(timeout=60_000)
	public void testRangeQuery() throws UnknownHostException, IOException {
		final Tuple tuple1 = new Tuple(KEY1, new Hyperrectangle(1.0d, 2.0d, 1.0d, 2.0d), KEY2.getBytes());
		final Tuple tuple2 = new Tuple(KEY2, new Hyperrectangle(10.0d, 20.0d, 10.0d, 20.0d), KEY2.getBytes());

		networkProxyClient.put(tuple1, TEST_TABLE_1);
		networkProxyClient.put(tuple2, TEST_TABLE_1);

		final List<Tuple> result1 = networkProxyClient.rangeQuery(new Hyperrectangle(1.5d, 1.6d, 1.5d, 1.6d), TEST_TABLE_1);
		Assert.assertEquals(1, result1.size());
		Assert.assertEquals(tuple1, result1.get(0));

		final List<Tuple> result2 = networkProxyClient.rangeQuery(new Hyperrectangle(15d, 16d, 15d, 16d), TEST_TABLE_1);
		Assert.assertEquals(1, result2.size());
		Assert.assertEquals(tuple2, result2.get(0));

		final List<Tuple> result3 = networkProxyClient.rangeQuery(Hyperrectangle.FULL_SPACE, TEST_TABLE_1);
		Assert.assertEquals(2, result3.size());
	}

	@Test(timeout=60_000)
	public void testRangeQueryLocal() throws UnknownHostException, IOException {
		final Tuple tuple1 = new Tuple(KEY1, new Hyperrectangle(1.0d, 2.0d, 1.0d, 2.0d), KEY2.getBytes());
		final Tuple tuple2 = new Tuple(KEY2, new Hyperrectangle(10.0d, 20.0d, 10.0d, 20.0d), KEY2.getBytes());

		networkProxyClient.put(tuple1, TEST_TABLE_1);
		networkProxyClient.put(tuple2, TEST_TABLE_1);

		final List<Tuple> result1 = networkProxyClient.rangeQueryLocal(new Hyperrectangle(1.5d, 1.6d, 1.5d, 1.6d), TEST_TABLE_1);
		Assert.assertEquals(1, result1.size());
		Assert.assertEquals(tuple1, result1.get(0));

		final List<Tuple> result2 = networkProxyClient.rangeQueryLocal(new Hyperrectangle(15d, 16d, 15d, 16d), TEST_TABLE_1);
		Assert.assertEquals(1, result2.size());
		Assert.assertEquals(tuple2, result2.get(0));

		final List<Tuple> result3 = networkProxyClient.rangeQueryLocal(Hyperrectangle.FULL_SPACE, TEST_TABLE_1);
		Assert.assertEquals(2, result3.size());
		Assert.assertTrue(result3.contains(tuple1));
		Assert.assertTrue(result3.contains(tuple2));
	}

	@Test(timeout=60_000)
	public void testJoin0() throws Exception {
		networkProxyClient.delete(KEY1, TEST_TABLE_1);
		networkProxyClient.delete(KEY2, TEST_TABLE_1);
		networkProxyClient.delete(KEY3, TEST_TABLE_1);

		final List<JoinedTuple> joinResult = networkProxyClient.join(
				Hyperrectangle.FULL_SPACE, TEST_TABLE_1, TEST_TABLE_2);

		Assert.assertTrue(joinResult.isEmpty());
	}

	@Test(timeout=60_000)
	public void testJoin1() throws Exception {
		final Tuple tuple1Table1 = new Tuple(KEY1, new Hyperrectangle(1.0d, 2.0d, 1.0d, 2.0d), "".getBytes());
		final Tuple tuple2Table1 = new Tuple(KEY2, new Hyperrectangle(5.0d, 6.0d, 5.0d, 6.0d), "".getBytes());
		final Tuple tuple1Table2 = new Tuple(KEY3, new Hyperrectangle(3.0d, 5.5d, 3.0d, 5.5d), "".getBytes());

		networkProxyClient.put(tuple1Table1, TEST_TABLE_1);
		networkProxyClient.put(tuple2Table1, TEST_TABLE_1);
		networkProxyClient.put(tuple1Table2, TEST_TABLE_2);

		final List<JoinedTuple> joinResult1 = networkProxyClient.join(
				new Hyperrectangle(100d, 200d, 100d, 200d), TEST_TABLE_1, TEST_TABLE_2);

		Assert.assertTrue(joinResult1.isEmpty());

		final List<JoinedTuple> joinResult2 = networkProxyClient.join(
				Hyperrectangle.FULL_SPACE, TEST_TABLE_1, TEST_TABLE_2);

		Assert.assertEquals(1, joinResult2.size());

		final JoinedTuple resultTuple = new JoinedTuple(Arrays.asList(tuple2Table1, tuple1Table2), Arrays.asList(TEST_TABLE_1, TEST_TABLE_2));
		Assert.assertEquals(resultTuple, joinResult2.get(0));
	}

	@Test(timeout=60_000)
	public void testJoin2() throws Exception {
		final Tuple tuple1Table1 = new Tuple(KEY1, new Hyperrectangle(1.0d, 2.0d, 1.0d, 2.0d), "".getBytes());
		final Tuple tuple2Table1 = new Tuple(KEY2, new Hyperrectangle(5.0d, 6.0d, 5.0d, 6.0d), "".getBytes());
		final Tuple tuple1Table2 = new Tuple(KEY3, new Hyperrectangle(3.0d, 5.5d, 3.0d, 5.5d), "".getBytes());

		networkProxyClient.put(tuple1Table1, TEST_TABLE_1);
		networkProxyClient.put(tuple2Table1, TEST_TABLE_1);
		networkProxyClient.put(tuple1Table2, TEST_TABLE_2);

		final List<JoinedTuple> joinResult1 = networkProxyClient.joinLocal(
				new Hyperrectangle(100d, 200d, 100d, 200d), TEST_TABLE_1, TEST_TABLE_2);

		Assert.assertTrue(joinResult1.isEmpty());

		final List<JoinedTuple> joinResult2 = networkProxyClient.joinLocal(
				Hyperrectangle.FULL_SPACE, TEST_TABLE_1, TEST_TABLE_2);

		Assert.assertEquals(1, joinResult2.size());

		final JoinedTuple resultTuple = new JoinedTuple(Arrays.asList(tuple2Table1, tuple1Table2), Arrays.asList(TEST_TABLE_1, TEST_TABLE_2));
		Assert.assertEquals(resultTuple, joinResult2.get(0));
	}

}

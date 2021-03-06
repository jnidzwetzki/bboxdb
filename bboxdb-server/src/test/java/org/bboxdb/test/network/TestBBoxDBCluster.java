/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.test.network;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousConstQueryPlan;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.util.EnvironmentHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBBoxDBCluster {


	/**
	 * The distribution group
	 */
	private static final String DISTRIBUTION_GROUP = "testgroup";

	/**
	 * The instance of the software
	 */
	private static BBoxDBMain bboxDBMain;


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
	 * Re-create distribution group for each test
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@Before
	public void before() throws InterruptedException, BBoxDBException {
		final BBoxDB bboxdbClient = EnvironmentHelper.connectToServer();
		EnvironmentHelper.recreateDistributionGroup(bboxdbClient, DISTRIBUTION_GROUP);
		disconnect(bboxdbClient);
	}

	/**
	 * Integration test for the disconnect package
	 * @throws InterruptedException
	 *
	 */
	@Test(timeout=60000)
	public void testSendDisconnectPackage() throws InterruptedException {
		System.out.println("=== Running cluster testSendDisconnectPackage");

		final BBoxDBCluster bboxdbClient = EnvironmentHelper.connectToServer();
		Assert.assertTrue(bboxdbClient.isConnected());
		bboxdbClient.close();
		Assert.assertFalse(bboxdbClient.isConnected());

		System.out.println("=== End cluster testSendDisconnectPackage");
		disconnect(bboxdbClient);
	}

	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testInsertAndBoundingBoxTimeQuery() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testInsertAndBoundingBoxTimeQuery");

		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();

		NetworkQueryHelper.executeBoudingboxAndTimeQuery(bboxDBClient, DISTRIBUTION_GROUP);

		System.out.println("=== End cluster testInsertAndBoundingBoxTimeQuery");

		disconnect(bboxDBClient);
	}

	/**
	 * The the insert and the deletion of a tuple
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testInsertAndDelete() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testInsertAndDelete");

		final BBoxDB bboxdbClient = EnvironmentHelper.connectToServer();

		NetworkQueryHelper.testInsertAndDeleteTuple(bboxdbClient, DISTRIBUTION_GROUP);
		System.out.println("=== End cluster testInsertAndDelete");

		disconnect(bboxdbClient);
	}

	/**
	 * Test the tuple join
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testJoin() throws InterruptedException, ExecutionException, BBoxDBException {
		System.out.println("=== Running cluster testJoin");

		final BBoxDB bboxdbClient = EnvironmentHelper.connectToServer();

		NetworkQueryHelper.executeJoinQuery(bboxdbClient, DISTRIBUTION_GROUP);

		System.out.println("=== End cluster testJoin");

		disconnect(bboxdbClient);
	}

	/**
	 * Execute the version time query
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	@Test(timeout=60000)
	public void testVersionTimeQuery() throws InterruptedException, BBoxDBException {
		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();

		NetworkQueryHelper.testVersionTimeQuery(bboxDBClient, DISTRIBUTION_GROUP);
		disconnect(bboxDBClient);
	}

	/**
	 * Execute the version inserted query
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	@Test(timeout=60000)
	public void testInsertedTimeQuery() throws InterruptedException, BBoxDBException {
		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();

		NetworkQueryHelper.testInsertedTimeQuery(bboxDBClient, DISTRIBUTION_GROUP);
		disconnect(bboxDBClient);
	}

	/**
	 * Disconnect from server
	 * @param bboxDBConnection
	 */
	protected void disconnect(final BBoxDB bboxDBClient) {
		bboxDBClient.close();
		Assert.assertFalse(bboxDBClient.isConnected());
	}

	/**
	 * Insert some tuples and start a bounding box query afterwards
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testInsertAndBoundingBoxContinousQuery() throws InterruptedException,
	ExecutionException, BBoxDBException {

		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();

		final String table = DISTRIBUTION_GROUP + "_relation9991";

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		// Execute query
		final Hyperrectangle bbox = new Hyperrectangle(-1d, 2d, -1d, 2d);
		final ContinuousConstQueryPlan constQueryPlan = new ContinuousConstQueryPlan(table, new ArrayList<>(), bbox, bbox, true);
		final JoinedTupleListFuture future = bboxDBClient.queryContinuous(constQueryPlan);

		Assert.assertFalse(future.isFailed());

		bboxDBClient.cancelQuery(future.getAllConnections());

		disconnect(bboxDBClient);
	}

	/**
	 * The the insert into a non existing table, insert should return immediately
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	@Test(timeout=20_000, expected = BBoxDBException.class)
	public void testInsertInNonExistingTable() throws InterruptedException, ExecutionException, BBoxDBException {
		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();
		
		final String table = DISTRIBUTION_GROUP + "_nonexistingtable";

		final Tuple tuple1 = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes());
		final EmptyResultFuture insertResult1 = bboxDBClient.insertTuple(table, tuple1);
		insertResult1.waitForCompletion();

		Assert.fail("No exception was thrown during insert");
		
		disconnect(bboxDBClient);
	}
	
	/**
	 * Test misc methods
	 * @throws InterruptedException
	 */
	@Test(timeout=60000)
	public void testMiscMethods() throws InterruptedException {
		final BBoxDB bboxDBClient = EnvironmentHelper.connectToServer();
		Assert.assertTrue(bboxDBClient.toString().length() > 10);
		Assert.assertTrue(bboxDBClient.getTuplesPerPage() >= -1);
		bboxDBClient.isPagingEnabled();
		disconnect(bboxDBClient);
	}
}

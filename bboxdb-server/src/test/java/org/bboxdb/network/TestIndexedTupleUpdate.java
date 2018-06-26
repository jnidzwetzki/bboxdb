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

import java.util.Optional;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.tools.IndexedTupleUpdateHelper;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIndexedTupleUpdate {
	/**
	 * The instance of the software
	 */
	private static BBoxDBMain bboxDBMain;
	
	/**
	 * The replication factor for the unit tests
	 */
	public final static short REPLICATION_FACTOR = 1;
	
	/**
	 * The cluster contact point
	 */
	private static final String CLUSTER_CONTACT_POINT = "localhost:2181";

	/**
	 * The distribution group
	 */
	private static final String DISTRIBUTION_GROUP = "testgroupindex";
	
	/**
	 * The table name
	 */
	private static final String TABLENAME = DISTRIBUTION_GROUP + "_testtable";
	
	/**
	 * The index table name
	 */
	private static final String TABLENAME_INDEX = IndexedTupleUpdateHelper.convertTablenameToIndexTablename(TABLENAME);
	
	
	@BeforeClass
	public static void init() throws Exception {
		bboxDBMain = new BBoxDBMain();
		bboxDBMain.init();
		bboxDBMain.start();
		
		// Allow connections to localhost (needed for travis CI test)
		MembershipConnectionService.getInstance().clearBlacklist();
						
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
		final BBoxDB bboxdbConnection = connectToServer();
		TestHelper.recreateDistributionGroup(bboxdbConnection, DISTRIBUTION_GROUP);
		
		final String indexGroupName = IndexedTupleUpdateHelper.IDX_DGROUP_PREFIX + DISTRIBUTION_GROUP;
		final EmptyResultFuture resultDelete = bboxdbConnection.deleteDistributionGroup(indexGroupName);
		resultDelete.waitForCompletion();
		Assert.assertFalse(resultDelete.isFailed());
		
		bboxdbConnection.createTable(TABLENAME, TupleStoreConfigurationBuilder.create().build());
		
		disconnect(bboxdbConnection);
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
	 * Build a new connection to the bboxdb server
	 * 
	 * @return
	 * @throws InterruptedException 
	 */
	protected BBoxDBCluster connectToServer() throws InterruptedException {
		final String clusterName = BBoxDBConfigurationManager.getConfiguration().getClustername();
		final BBoxDBCluster bboxdbCluster = new BBoxDBCluster(CLUSTER_CONTACT_POINT, clusterName);
	
		final boolean result = bboxdbCluster.connect();
		Assert.assertTrue(result);
		
		Thread.sleep(50);
		
		Assert.assertTrue(bboxdbCluster.isConnected());
		
		return bboxdbCluster;
	}
	
	
	@Test
	public void testIndexUpdate1() throws InterruptedException, 
		ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		
		final BBoxDBCluster cluster = connectToServer();
		
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(4d, 5d, 4d, 5d), "value1".getBytes());
		
		final IndexedTupleUpdateHelper indexedTupleUpdateHelper = new IndexedTupleUpdateHelper(cluster);
		indexedTupleUpdateHelper.createMissingTables(TABLENAME);
		
		final Optional<Tuple> oldIndexEntry = indexedTupleUpdateHelper.getOldIndexEntry(TABLENAME_INDEX, tuple1.getKey());
		final byte[] boundingBoxData1 = oldIndexEntry.get().getDataBytes();
		final Hyperrectangle boundingBox1 = new Hyperrectangle(new String(boundingBoxData1));
		
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, boundingBox1);
		
		disconnect(cluster);
	}
	
	@Test
	public void testIndexUpdate2() throws InterruptedException, 
		ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		
		final BBoxDBCluster cluster = connectToServer();
		
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(4d, 5d, 4d, 5d), "value1".getBytes());
		
		final IndexedTupleUpdateHelper indexedTupleUpdateHelper = new IndexedTupleUpdateHelper(cluster);
		
		final EmptyResultFuture update1Future = indexedTupleUpdateHelper.handleTupleUpdate(TABLENAME, tuple1);
		update1Future.waitForCompletion();
		Assert.assertFalse(update1Future.isFailed());
		indexedTupleUpdateHelper.waitForCompletion();
		
		final Optional<Tuple> oldIndexEntry1 = indexedTupleUpdateHelper.getOldIndexEntry(TABLENAME_INDEX, tuple1.getKey());
		final byte[] boundingBoxData1 = oldIndexEntry1.get().getDataBytes();
		final Hyperrectangle boundingBox1 = new Hyperrectangle(new String(boundingBoxData1));
		Assert.assertEquals(tuple1.getBoundingBox(), boundingBox1);
		
		final Tuple tuple2 = new Tuple("abc", new Hyperrectangle(7d, 9d, 4d, 50d), "value2".getBytes());

		final EmptyResultFuture update2Future = indexedTupleUpdateHelper.handleTupleUpdate(TABLENAME, tuple2);
		update2Future.waitForCompletion();
		Assert.assertFalse(update2Future.isFailed());
		indexedTupleUpdateHelper.waitForCompletion();

		final Optional<Tuple> oldIndexEntry2 = indexedTupleUpdateHelper.getOldIndexEntry(TABLENAME_INDEX, tuple2.getKey());
		final byte[] boundingBoxData2 = oldIndexEntry2.get().getDataBytes();
		final Hyperrectangle boundingBox2 = new Hyperrectangle(new String(boundingBoxData2));
		Assert.assertEquals(tuple2.getBoundingBox(), boundingBox2);
		
		disconnect(cluster);
	}
}

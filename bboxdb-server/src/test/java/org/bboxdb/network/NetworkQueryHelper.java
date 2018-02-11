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

import java.util.Arrays;
import java.util.List;

import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class NetworkQueryHelper {

	/**
	 * The configuration for the unit tests
	 */
	public static DistributionGroupConfiguration getConfiguration(final int dimensions) {
		return DistributionGroupConfigurationBuilder
				.create(dimensions)
				.withReplicationFactor((short) 1)
				.build();
	}
	
	/**
	 * Execute a bounding box and time query
	 * @param bboxDBClient
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 */
	public static void executeBoudingboxAndTimeQuery(final BBoxDB bboxDBClient) 
			throws BBoxDBException, InterruptedException {
		
		final String distributionGroup = "testgroup"; 
		final String table = distributionGroup + "_relation9990";
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, 
				getConfiguration(2));
		
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForAll();
		Assert.assertFalse(resultCreateTable.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes(), 4);
		final EmptyResultFuture result1 = bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes(), 4);
		final EmptyResultFuture result2 = bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes(), 1);
		final EmptyResultFuture result3 = bboxDBClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		final EmptyResultFuture result4 = bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		final EmptyResultFuture result5 = bboxDBClient.insertTuple(table, tuple5);
		
		result1.waitForAll();
		result2.waitForAll();
		result3.waitForAll();
		result4.waitForAll();
		result5.waitForAll();

		final TupleListFuture future = bboxDBClient.queryBoundingBoxAndTime(table, new BoundingBox(-1d, 2d, -1d, 2d), 2);
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());
		
		Assert.assertEquals(3, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
	}
	
	/**
	 * Test a bounding box query
	 * @param bboxDBClient
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testBoundingBoxQuery(final BBoxDB bboxDBClient) 
			throws BBoxDBException, InterruptedException {
		
		System.out.println("=== Running testInsertAndBoundingBoxQuery");
		final String distributionGroup = "testgroup"; 
		final String table = distributionGroup + "_relation9991";
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, 
				getConfiguration(2));
		
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		
		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForAll();
		Assert.assertFalse(resultCreateTable.isFailed());
		
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes());
		final EmptyResultFuture result1 = bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes());
		final EmptyResultFuture result2 = bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes());
		final EmptyResultFuture result3 = bboxDBClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		final EmptyResultFuture result4 = bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		final EmptyResultFuture result5 = bboxDBClient.insertTuple(table, tuple5);

		result1.waitForAll();
		result2.waitForAll();
		result3.waitForAll();
		result4.waitForAll();
		result5.waitForAll();
		
		final TupleListFuture future = bboxDBClient.queryBoundingBox(table, new BoundingBox(-1d, 2d, -1d, 2d));
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());
		
		Assert.assertEquals(3, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
		System.out.println("=== End testInsertAndBoundingBoxQuery");
	}

	/**
	 * Inset and delete tuple
	 * @param bboxDBClient
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testInsertAndDeleteTuple(final BBoxDB bboxDBClient) 
			throws BBoxDBException, InterruptedException {
		
		System.out.println("=== Running testInsertAndDelete");

		final String distributionGroup = "testgroupdel"; 
		final String table = distributionGroup + "_relation4";
		final String key = "key12";
		
		// Delete distribution group
		System.out.println("Delete distribution group");
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		System.out.println("Create distribution group");
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, 
				getConfiguration(1));
		
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForAll();
		Assert.assertFalse(resultCreateTable.isFailed());
		
		System.out.println("Delete tuple");
		final EmptyResultFuture deleteResult1 = bboxDBClient.deleteTuple(table, key);
		deleteResult1.waitForAll();
		Assert.assertFalse(deleteResult1.isFailed());
		Assert.assertTrue(deleteResult1.isDone());
		
		System.out.println("Query key");
		final TupleListFuture getResult = bboxDBClient.queryKey(table, key);
		getResult.waitForAll();
		Assert.assertFalse(getResult.isFailed());
		Assert.assertTrue(getResult.isDone());
		
		System.out.println("Insert tuple");
		final Tuple tuple = new Tuple(key, BoundingBox.EMPTY_BOX, "abc".getBytes());
		final EmptyResultFuture insertResult = bboxDBClient.insertTuple(table, tuple);
		insertResult.waitForAll();
		Assert.assertFalse(insertResult.isFailed());
		Assert.assertTrue(insertResult.isDone());

		System.out.println("Query key 2");
		final TupleListFuture getResult2 = bboxDBClient.queryKey(table, key);
		getResult2.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(getResult2.iterator());
		Assert.assertEquals(tuple, resultList.get(0));

		System.out.println("Delete tuple 2");
		final EmptyResultFuture deleteResult2 = bboxDBClient.deleteTuple(table, key, System.currentTimeMillis());
		deleteResult2.waitForAll();
		Assert.assertFalse(deleteResult2.isFailed());
		Assert.assertTrue(deleteResult2.isDone());
		
		System.out.println("Query key 3");
		final TupleListFuture getResult3 = bboxDBClient.queryKey(table, key);
		getResult3.waitForAll();
		Assert.assertFalse(getResult3.isFailed());
		Assert.assertTrue(getResult3.isDone());
		
		bboxDBClient.disconnect();
		
		System.out.println("=== End testInsertAndDelete");
	}

	/**
	 * Execute a join
	 * @param bboxDBClient
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	public static void executeJoinQuery(final BBoxDB bboxDBClient) 
			throws InterruptedException, BBoxDBException {
		
		System.out.println("=== Execute join");
		
		final String distributionGroup = "testgroupjoin"; 
		final String table1 = distributionGroup + "_table1";
		final String table2 = distributionGroup + "_table2";
		
		// Delete distribution group
		System.out.println("Delete distribution group");
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		System.out.println("Create distribution group");
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, 
				getConfiguration(2));
		
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Create table1
		System.out.println("Create table 1");
		final EmptyResultFuture resultCreateTable1 = bboxDBClient.createTable(table1, new TupleStoreConfiguration());
		resultCreateTable1.waitForAll();
		Assert.assertFalse(resultCreateTable1.isFailed());
		
		System.out.println("Create table 2");
		final EmptyResultFuture resultCreateTable2 = bboxDBClient.createTable(table2, new TupleStoreConfiguration());
		resultCreateTable2.waitForAll();
		Assert.assertFalse(resultCreateTable2.isFailed());
				
		// Insert tuples
		System.out.println("Insert tuple 1");
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(1.0, 2.0, 1.0, 2.0), "abc".getBytes());
		final EmptyResultFuture insertResult1 = bboxDBClient.insertTuple(table1, tuple1);
		insertResult1.waitForAll();
		Assert.assertFalse(insertResult1.isFailed());
		Assert.assertTrue(insertResult1.isDone());

		System.out.println("Insert tuple 2");
		final Tuple tuple2 = new Tuple("def", new BoundingBox(1.5, 2.5, 1.5, 2.5), "abc".getBytes());
		final EmptyResultFuture insertResult2 = bboxDBClient.insertTuple(table1, tuple2);
		insertResult2.waitForAll();
		Assert.assertFalse(insertResult2.isFailed());
		Assert.assertTrue(insertResult2.isDone());

		System.out.println("Insert tuple 3");
		final Tuple tuple3 = new Tuple("123", new BoundingBox(0.0, 5.0, 0.0, 5.0), "abc".getBytes());
		final EmptyResultFuture insertResult3 = bboxDBClient.insertTuple(table2, tuple3);
		insertResult3.waitForAll();
		Assert.assertFalse(insertResult3.isFailed());
		Assert.assertTrue(insertResult3.isDone());
		
		Thread.sleep(10000);
		
		// Execute the join
		final JoinedTupleListFuture joinResult = bboxDBClient.queryJoin(Arrays.asList(table1, table2), new BoundingBox(0.0, 10.0, 0.0, 10.0));
		joinResult.waitForAll();
		final List<JoinedTuple> resultList = Lists.newArrayList(joinResult.iterator());

		System.out.println(resultList);
		Assert.assertEquals(2, resultList.size());
		Assert.assertEquals(2, resultList.get(0).getNumberOfTuples());
		Assert.assertEquals(table1, resultList.get(0).getTupleStoreName(0));
		Assert.assertEquals(table2, resultList.get(0).getTupleStoreName(1));
		Assert.assertEquals(new BoundingBox(1.0, 2.0, 1.0, 2.0), resultList.get(0).getBoundingBox());

		Assert.assertEquals(2, resultList.get(1).getNumberOfTuples());
		Assert.assertEquals(table1, resultList.get(1).getTupleStoreName(0));
		Assert.assertEquals(table2, resultList.get(1).getTupleStoreName(1));
		Assert.assertEquals(new BoundingBox(1.5, 2.5, 1.5, 2.5), resultList.get(1).getBoundingBox());

		System.out.println("=== End Execute join");
	}

}

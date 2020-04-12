/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class NetworkQueryHelper {

	/**
	 * Execute a bounding box and time query
	 * @param bboxDBConnection
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void executeBoudingboxAndTimeQuery(final BBoxDB bboxDBClient,
			final String distributionGroup) throws BBoxDBException, InterruptedException {

		final String table = distributionGroup + "_relation9990";

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(0d, 1d, 0d, 1d), "abc".getBytes(), 4);
		final EmptyResultFuture result1 = bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(0d, 0.5d, 0d, 0.5d), "def".getBytes(), 4);
		final EmptyResultFuture result2 = bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new Hyperrectangle(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes(), 1);
		final EmptyResultFuture result3 = bboxDBClient.insertTuple(table, tuple3);

		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new Hyperrectangle(-10d, -9d, -10d, -9d), "ijk".getBytes());
		final EmptyResultFuture result4 = bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new Hyperrectangle(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		final EmptyResultFuture result5 = bboxDBClient.insertTuple(table, tuple5);

		result1.waitForCompletion();
		result2.waitForCompletion();
		result3.waitForCompletion();
		result4.waitForCompletion();
		result5.waitForCompletion();

		final TupleListFuture future = bboxDBClient.queryRectangleAndTime(table, new Hyperrectangle(-1d, 2d, -1d, 2d), 2);
		future.waitForCompletion();
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
	 * @param bboxDBConnection
	 * @return
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testBoundingBoxQuery(final BBoxDB bboxDBClient, final String distributionGroup,
			final boolean withTupes) throws BBoxDBException, InterruptedException {

		System.out.println("=== Running testInsertAndBoundingBoxQuery");
		final String table = distributionGroup + "_relation9991";

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(0d, 1d, 0d, 1d), "abc".getBytes());
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(0d, 0.5d, 0d, 0.5d), "def".getBytes());
		final Tuple tuple3 = new Tuple("geh", new Hyperrectangle(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes());

		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new Hyperrectangle(-10d, -9d, -10d, -9d), "ijk".getBytes());
		final Tuple tuple5 = new Tuple("lmn", new Hyperrectangle(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());

		if(withTupes) {
			final EmptyResultFuture result1 = bboxDBClient.insertTuple(table, tuple1);
			final EmptyResultFuture result2 = bboxDBClient.insertTuple(table, tuple2);
			final EmptyResultFuture result3 = bboxDBClient.insertTuple(table, tuple3);
			final EmptyResultFuture result4 = bboxDBClient.insertTuple(table, tuple4);
			final EmptyResultFuture result5 = bboxDBClient.insertTuple(table, tuple5);

			result1.waitForCompletion();
			result2.waitForCompletion();
			result3.waitForCompletion();
			result4.waitForCompletion();
			result5.waitForCompletion();
		}

		final Hyperrectangle queryBox1 = new Hyperrectangle(-1d, 2d, -1d, 2d);

		final List<Tuple> resultList1 = executeBBoxQuery(bboxDBClient, table, queryBox1);

		if(! withTupes) {
			Assert.assertEquals(0, resultList1.size());
		} else {
			Assert.assertEquals(3, resultList1.size());
			Assert.assertTrue(resultList1.contains(tuple1));
			Assert.assertTrue(resultList1.contains(tuple2));
			Assert.assertTrue(resultList1.contains(tuple3));
			Assert.assertFalse(resultList1.contains(tuple4));
			Assert.assertFalse(resultList1.contains(tuple5));
		}

		// Query complete space
		final List<Tuple> resultList2 = executeBBoxQuery(bboxDBClient, table, Hyperrectangle.FULL_SPACE);

		if(! withTupes) {
			Assert.assertEquals(0, resultList2.size());
		} else {
			Assert.assertEquals(5, resultList2.size());
			Assert.assertTrue(resultList2.contains(tuple1));
			Assert.assertTrue(resultList2.contains(tuple2));
			Assert.assertTrue(resultList2.contains(tuple3));
			Assert.assertTrue(resultList2.contains(tuple4));
			Assert.assertTrue(resultList2.contains(tuple5));
		}


		System.out.println("=== End testInsertAndBoundingBoxQuery");
	}

	/**
	 * Execute the bounding box query
	 * @param bboxDBClient
	 * @param table
	 * @param queryBox
	 * @return
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private static List<Tuple> executeBBoxQuery(final BBoxDB bboxDBClient, final String table,
			final Hyperrectangle queryBox) throws BBoxDBException, InterruptedException {

		System.out.println("=== Executing query for: " + queryBox);
		final TupleListFuture future = bboxDBClient.queryRectangle(table, queryBox, "", "".getBytes());
		future.waitForCompletion();
		System.out.println("=== Query DONE");

		Assert.assertTrue(future.isDone());
		Assert.assertFalse(future.isFailed());

		return Lists.newArrayList(future.iterator());
	}

	/**
	 * Test a bounding box query
	 * @param bboxDBConnection
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testBoundingBoxQueryContinous1(final BBoxDBClient bboxDBClient, final String distributionGroup)
			throws BBoxDBException, InterruptedException {

		System.out.println("=== Running testBoundingBoxQueryContinous 1");
		final String table = distributionGroup + "_relation9991";

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		final ContinuousQueryPlan constQueryPlan = QueryPlanBuilder
				.createQueryOnTable(table)
				.forAllNewTuplesStoredInRegion(new Hyperrectangle(-1d, 2d, -1d, 2d))
				.compareWithStaticRegion(new Hyperrectangle(-1d, 2d, -1d, 2d))
				.build();

		final JoinedTupleListFuture queryFuture = bboxDBClient.queryContinuous(constQueryPlan);

		final int tuples = 10;

		for(int i = 0; i < tuples; i++) {
			final Tuple tuple = new Tuple("1", new Hyperrectangle(0d, 1d, 0d, 1d), "".getBytes());
			final EmptyResultFuture insertResult = bboxDBClient.insertTuple(table, tuple);
			insertResult.waitForCompletion();
		}

		// Wait for page full
		System.out.println("=== Wait for query result");
		queryFuture.waitForCompletion();

		final List<JoinedTuple> resultList = new ArrayList<>();
		final Iterator<JoinedTuple> iterator = queryFuture.iterator();

		for(int i = 0; i < tuples; i++) {
			if(iterator.hasNext()) {
				resultList.add(iterator.next());
			}
		}

		Assert.assertEquals(10, resultList.size());

		bboxDBClient.cancelQuery(queryFuture.getAllConnections());

		System.out.println("=== End testBoundingBoxQueryContinous 1");
	}

	/**
	 * Test a bounding box query
	 * @param bboxDBConnection
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testBoundingBoxQueryContinous2(final BBoxDBClient bboxDBClient, final String distributionGroup)
			throws BBoxDBException, InterruptedException {

		System.out.println("=== Running testBoundingBoxQueryContinous 2");

		final String table = distributionGroup + "_relation9992";

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		final ContinuousQueryPlan constQueryPlan = QueryPlanBuilder
				.createQueryOnTable(table)
				.forAllNewTuplesStoredInRegion(new Hyperrectangle(-1d, 2d, -1d, 2d))
				.compareWithTable(table)
				.build();

		final Tuple storedTuple = new Tuple("abc", new Hyperrectangle(0.5d, 1d, 0.5d, 1d), "".getBytes());
		final EmptyResultFuture storeResult = bboxDBClient.insertTuple(table, storedTuple);
		storeResult.waitForCompletion();
		Assert.assertFalse(storeResult.isFailed());
		Assert.assertTrue(storeResult.isDone());
		System.out.println("=== Insert first tuple done");

		final JoinedTupleListFuture queryFuture = bboxDBClient.queryContinuous(constQueryPlan);

		final Tuple tuple = new Tuple("1", new Hyperrectangle(0d, 1d, 0d, 1d), "".getBytes());
		final EmptyResultFuture insertResult = bboxDBClient.insertTuple(table, tuple);
		insertResult.waitForCompletion();
		System.out.println("=== Insert second tuple done");

		// Wait for page full
		System.out.println("=== Wait for query result");
		queryFuture.waitForCompletion();

		final Iterator<JoinedTuple> iterator = queryFuture.iterator();
		Assert.assertTrue(iterator.hasNext());

		final JoinedTuple foundTuple = iterator.next();

		Assert.assertEquals(2, foundTuple.getNumberOfTuples());
		Assert.assertEquals(table, foundTuple.getTupleStoreName(0));
		Assert.assertEquals(table, foundTuple.getTupleStoreName(1));
		Assert.assertEquals(tuple, foundTuple.getTuple(0));
		Assert.assertEquals(storedTuple, foundTuple.getTuple(1));

		bboxDBClient.cancelQuery(queryFuture.getAllConnections());

		System.out.println("=== End testBoundingBoxQueryContinous 2");
	}

	/**
	 * Insert and delete tuple
	 * @param bboxDBConnection
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testInsertAndDeleteTuple(final BBoxDB bboxDBClient, final String distributionGroup)
			throws BBoxDBException, InterruptedException {

		System.out.println("=== Running testInsertAndDelete");

		final String table = distributionGroup + "_relation4";
		final String key = "key12";

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		System.out.println("Delete tuple");
		final EmptyResultFuture deleteResult1 = bboxDBClient.deleteTuple(table, key);
		deleteResult1.waitForCompletion();
		Assert.assertFalse(deleteResult1.isFailed());
		Assert.assertTrue(deleteResult1.isDone());

		System.out.println("Query key");
		final TupleListFuture getResult = bboxDBClient.queryKey(table, key);
		getResult.waitForCompletion();
		Assert.assertFalse(getResult.isFailed());
		Assert.assertTrue(getResult.isDone());

		System.out.println("Insert tuple");
		final Tuple tuple = new Tuple(key, Hyperrectangle.FULL_SPACE, "abc".getBytes());
		final EmptyResultFuture insertResult = bboxDBClient.insertTuple(table, tuple);
		insertResult.waitForCompletion();
		Assert.assertFalse(insertResult.isFailed());
		Assert.assertTrue(insertResult.isDone());

		System.out.println("Query key 2");
		final TupleListFuture getResult2 = bboxDBClient.queryKey(table, key);
		getResult2.waitForCompletion();
		final List<Tuple> resultList = Lists.newArrayList(getResult2.iterator());
		Assert.assertEquals(tuple, resultList.get(0));

		System.out.println("Delete tuple 2");
		final EmptyResultFuture deleteResult2 = bboxDBClient.deleteTuple(table, key, System.currentTimeMillis());
		deleteResult2.waitForCompletion();
		Assert.assertFalse(deleteResult2.isFailed());
		Assert.assertTrue(deleteResult2.isDone());

		System.out.println("Query key 3");
		final TupleListFuture getResult3 = bboxDBClient.queryKey(table, key);
		getResult3.waitForCompletion();
		Assert.assertFalse(getResult3.isFailed());
		Assert.assertTrue(getResult3.isDone());

		bboxDBClient.close();

		System.out.println("=== End testInsertAndDelete");
	}

	/**
	 * Execute a join
	 * @param bboxDBConnection
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	public static void executeJoinQuery(final BBoxDB bboxDBClient, final String distributionGroup)
			throws InterruptedException, BBoxDBException {

		System.out.println("=== Execute join");

		final String table1 = distributionGroup + "_table1";
		final String table2 = distributionGroup + "_table2";

		// Create table1
		System.out.println("Create table 1");
		final EmptyResultFuture resultCreateTable1 = bboxDBClient.createTable(table1, new TupleStoreConfiguration());
		resultCreateTable1.waitForCompletion();
		Assert.assertFalse(resultCreateTable1.isFailed());

		System.out.println("Create table 2");
		final EmptyResultFuture resultCreateTable2 = bboxDBClient.createTable(table2, new TupleStoreConfiguration());
		resultCreateTable2.waitForCompletion();
		Assert.assertFalse(resultCreateTable2.isFailed());

		// Insert tuples
		System.out.println("Insert tuple 1");
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1.0, 7.0, 1.0, 7.0), "abc".getBytes());
		final EmptyResultFuture insertResult1 = bboxDBClient.insertTuple(table1, tuple1);
		insertResult1.waitForCompletion();
		Assert.assertFalse(insertResult1.isFailed());
		Assert.assertTrue(insertResult1.isDone());

		System.out.println("Insert tuple 2");
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "abc".getBytes());
		final EmptyResultFuture insertResult2 = bboxDBClient.insertTuple(table1, tuple2);
		insertResult2.waitForCompletion();
		Assert.assertFalse(insertResult2.isFailed());
		Assert.assertTrue(insertResult2.isDone());

		System.out.println("Insert tuple 3");
		final Tuple tuple3 = new Tuple("123", new Hyperrectangle(0.0, 5.0, 0.0, 5.0), "abc".getBytes());
		final EmptyResultFuture insertResult3 = bboxDBClient.insertTuple(table2, tuple3);
		insertResult3.waitForCompletion();
		Assert.assertFalse(insertResult3.isFailed());
		Assert.assertTrue(insertResult3.isDone());

		// Execute the join
		final JoinedTupleListFuture joinResult = bboxDBClient.queryJoin(Arrays.asList(table1, table2),
				new Hyperrectangle(0.0, 10.0, 0.0, 10.0), "", "".getBytes());
		joinResult.waitForCompletion();
		final List<JoinedTuple> resultList = Lists.newArrayList(joinResult.iterator());

		System.out.println(resultList);
		Assert.assertEquals(2, resultList.size());
		Assert.assertEquals(2, resultList.get(0).getNumberOfTuples());
		Assert.assertEquals(table1, resultList.get(0).getTupleStoreName(0));
		Assert.assertEquals(table2, resultList.get(0).getTupleStoreName(1));
		Assert.assertEquals(new Hyperrectangle(0.0, 7.0, 0.0, 7.0), resultList.get(0).getBoundingBox());

		Assert.assertEquals(2, resultList.get(1).getNumberOfTuples());
		Assert.assertEquals(table1, resultList.get(1).getTupleStoreName(0));
		Assert.assertEquals(table2, resultList.get(1).getTupleStoreName(1));
		Assert.assertEquals(new Hyperrectangle(0.0, 5.0, 0.0, 5.0), resultList.get(1).getBoundingBox());

		System.out.println("=== End Execute join");
	}

	/**
	 * Test the version time query
	 * @param bboxDBConnection
	 * @param distributionGroup
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	public static void testVersionTimeQuery(final BBoxDB bboxDBClient,
			final String distributionGroup) throws InterruptedException, BBoxDBException {

		final String table = distributionGroup + "_relationqt";
		final String key = "key12";

		System.out.println("== Executing testVersionTimeQuery");

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		final Tuple tuple = new Tuple(key, Hyperrectangle.FULL_SPACE, "abc".getBytes());
		final EmptyResultFuture insertResult = bboxDBClient.insertTuple(table, tuple);
		insertResult.waitForCompletion();
		Assert.assertFalse(insertResult.isFailed());
		Assert.assertTrue(insertResult.isDone());

		System.out.println("Query all versions");
		final TupleListFuture getResult1 = bboxDBClient.queryVersionTime(table, 0);
		getResult1.waitForCompletion();
		final List<Tuple> resultList1 = Lists.newArrayList(getResult1.iterator());
		Assert.assertEquals(1, resultList1.size());
		Assert.assertEquals(tuple, resultList1.get(0));

		System.out.println("Query newest versions");
		final TupleListFuture getResult2 = bboxDBClient.queryVersionTime(table, Long.MAX_VALUE);
		getResult2.waitForCompletion();
		final List<Tuple> resultList2 = Lists.newArrayList(getResult2.iterator());
		Assert.assertTrue(resultList2.isEmpty());
	}

	/**
	 * Test the version time query
	 * @param bboxDBConnection
	 * @param distributionGroup
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 */
	public static void testInsertedTimeQuery(final BBoxDB bboxDBClient,
			final String distributionGroup) throws InterruptedException, BBoxDBException {

		final String table = distributionGroup + "_relationit";
		final String key = "key12";

		System.out.println("== Executing testInsertedTimeQuery");

		// Create table
		final EmptyResultFuture resultCreateTable = bboxDBClient.createTable(table, new TupleStoreConfiguration());
		resultCreateTable.waitForCompletion();
		Assert.assertFalse(resultCreateTable.isFailed());

		final Tuple tuple = new Tuple(key, Hyperrectangle.FULL_SPACE, "abc".getBytes());
		final EmptyResultFuture insertResult = bboxDBClient.insertTuple(table, tuple);
		insertResult.waitForCompletion();
		Assert.assertFalse(insertResult.isFailed());
		Assert.assertTrue(insertResult.isDone());

		System.out.println("Query all versions");
		final TupleListFuture getResult1 = bboxDBClient.queryInsertedTime(table, 0);
		getResult1.waitForCompletion();
		final List<Tuple> resultList1 = Lists.newArrayList(getResult1.iterator());
		Assert.assertEquals(1, resultList1.size());
		Assert.assertEquals(tuple, resultList1.get(0));

		System.out.println("Query newest versions");
		final TupleListFuture getResult2 = bboxDBClient.queryInsertedTime(table, Long.MAX_VALUE);
		getResult2.waitForCompletion();
		final List<Tuple> resultList2 = Lists.newArrayList(getResult2.iterator());
		Assert.assertTrue(resultList2.isEmpty());
	}
}

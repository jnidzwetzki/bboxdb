/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.util.List;

import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class NetworkQueryHelper {

	/**
	 * The replication factor for the unit tests
	 */
	public final static short REPLICATION_FACTOR = 1;
	
	
	/**
	 * Execute a bounding box and time query
	 * @param bboxDBClient
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 */
	public static void executeBoudingboxAndTimeQuery(final BBoxDB bboxDBClient) throws BBoxDBException, InterruptedException {
		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes(), 4);
		bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes(), 4);
		bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes(), 1);
		bboxDBClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		bboxDBClient.insertTuple(table, tuple5);

		final TupleListFuture future = bboxDBClient.queryBoundingBoxAndTime(table, new BoundingBox(-1d, 2d, -1d, 2d), 2);
		future.waitForAll();
		final List<Tuple> resultList = Lists.newArrayList(future.iterator());
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertTrue(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertFalse(resultList.contains(tuple3));
		Assert.assertFalse(resultList.contains(tuple4));
		Assert.assertFalse(resultList.contains(tuple5));
	}
	
	/**
	 * Test a bounding box query
	 * @param bboxDBClient
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void testBoundingBoxQuery(final BBoxDB bboxDBClient) throws BBoxDBException, InterruptedException {
		System.out.println("=== Running testInsertAndBoundingBoxQuery");
		final String distributionGroup = "2_testgroup"; 
		final String table = distributionGroup + "_relation9999";
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = bboxDBClient.deleteDistributionGroup(distributionGroup);
		resultDelete.waitForAll();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final EmptyResultFuture resultCreate = bboxDBClient.createDistributionGroup(distributionGroup, REPLICATION_FACTOR);
		resultCreate.waitForAll();
		Assert.assertFalse(resultCreate.isFailed());
		
		// Inside our bbox query
		final Tuple tuple1 = new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes());
		bboxDBClient.insertTuple(table, tuple1);
		final Tuple tuple2 = new Tuple("def", new BoundingBox(0d, 0.5d, 0d, 0.5d), "def".getBytes());
		bboxDBClient.insertTuple(table, tuple2);
		final Tuple tuple3 = new Tuple("geh", new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d), "geh".getBytes());
		bboxDBClient.insertTuple(table, tuple3);
		
		// Outside our bbox query
		final Tuple tuple4 = new Tuple("ijk", new BoundingBox(-10d, -9d, -10d, -9d), "ijk".getBytes());
		bboxDBClient.insertTuple(table, tuple4);
		final Tuple tuple5 = new Tuple("lmn", new BoundingBox(1000d, 1001d, 1000d, 1001d), "lmn".getBytes());
		bboxDBClient.insertTuple(table, tuple5);

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

}

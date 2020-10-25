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
package org.bboxdb.test.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.operator.BoundingBoxSelectOperator;
import org.bboxdb.storage.queryprocessor.operator.FullTablescanOperator;
import org.bboxdb.storage.queryprocessor.operator.Operator;
import org.bboxdb.storage.queryprocessor.operator.SpatialIndexReadOperator;
import org.bboxdb.storage.queryprocessor.operator.join.IndexedSpatialJoinOperator;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;


public class TestQueryProcessing {

	/**
	 * The 1. table name for tests
	 */
	private static final TupleStoreName TABLE_1 = new TupleStoreName("junitgroup_table1");

	/**
	 * The 2. table name for tests
	 */
	private static final TupleStoreName TABLE_2 = new TupleStoreName("junitgroup_table2");

	/**
	 * The 3. table name for tests
	 */
	private static final TupleStoreName TABLE_3 = new TupleStoreName("junitgroup_table3");

	/**
	 * The storage registry
	 */
	private static TupleStoreManagerRegistry storageRegistry;

	@BeforeClass
	public static void beforeClass() throws InterruptedException, BBoxDBException {
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
	}

	@AfterClass
	public static void afterClass() {
		if(storageRegistry != null) {
			storageRegistry.shutdown();
			storageRegistry = null;
		}
	}

	@Before
	public void init() throws StorageManagerException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_2);
		storageRegistry.createTable(TABLE_2, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_3);
		storageRegistry.createTable(TABLE_3, new TupleStoreConfiguration());
	}

	/**
	 * Simple BBox query
	 * @throws StorageManagerException
	 * @throws RejectedException
	 * @throws IOException
	 */
	@Test(timeout=60000)
	public void testBBoxQuery0() throws StorageManagerException, RejectedException, IOException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", Hyperrectangle.FULL_SPACE, "value".getBytes());
		final Tuple tuple2 = new Tuple("2", Hyperrectangle.FULL_SPACE, "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", Hyperrectangle.FULL_SPACE, "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		storageManager.put(tuple3);

		final Hyperrectangle queryBoundingBox = Hyperrectangle.FULL_SPACE;
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<MultiTuple> iterator = queryPlan.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);
		final List<Tuple> resultTupleList = resultList.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultTupleList.contains(tuple1));
		Assert.assertTrue(resultTupleList.contains(tuple2));
		Assert.assertTrue(resultTupleList.contains(tuple3));
	}

	/**
	 * Simple BBox query
	 * @throws StorageManagerException
	 * @throws RejectedException
	 * @throws IOException
	 */
	@Test(timeout=60000)
	public void testBBoxQuery1() throws StorageManagerException, RejectedException, IOException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		storageManager.put(tuple3);

		final Hyperrectangle queryBoundingBox = new Hyperrectangle(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<MultiTuple> iterator = queryPlan.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);
		final List<Tuple> resultTupleList = resultList.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultTupleList.contains(tuple1));
		Assert.assertTrue(resultTupleList.contains(tuple2));
		Assert.assertTrue(resultTupleList.contains(tuple3));

		// Reopen
		final Iterator<MultiTuple> iterator2 = queryPlan.iterator();

		final List<MultiTuple> resultList2 = Lists.newArrayList(iterator2);
		final List<Tuple> resultTupleList2 = resultList2.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList2.size());
		Assert.assertFalse(resultTupleList2.contains(tuple1));
		Assert.assertTrue(resultTupleList2.contains(tuple2));
		Assert.assertTrue(resultTupleList2.contains(tuple3));
	}

	/**
	 * Simple BBox query - across multiple tables
	 * @throws StorageManagerException
	 * @throws RejectedException
	 * @throws IOException
	 */
	@Test(timeout=60000)
	public void testBBoxQuery2() throws StorageManagerException, RejectedException, IOException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.initNewMemtable();
		storageManager.put(tuple2);
		storageManager.initNewMemtable();
		storageManager.put(tuple3);
		storageManager.initNewMemtable();

		final Hyperrectangle queryBoundingBox = new Hyperrectangle(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<MultiTuple> iterator = queryPlan.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);
		final List<Tuple> resultTupleList = resultList.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultTupleList.contains(tuple1));
		Assert.assertTrue(resultTupleList.contains(tuple2));
		Assert.assertTrue(resultTupleList.contains(tuple3));
	}

	/**
	 * Simple BBox query - across multiple tables on disk
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws RejectedException
	 * @throws IOException
	 */
	@Test(timeout=60000)
	public void testBBoxQuery3() throws StorageManagerException, InterruptedException, RejectedException, IOException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.flush();

		storageManager.put(tuple2);
		storageManager.flush();

		storageManager.put(tuple3);
		storageManager.flush();

		final Hyperrectangle queryBoundingBox = new Hyperrectangle(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<MultiTuple> iterator = queryPlan.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);
		final List<Tuple> resultTupleList = resultList.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultTupleList.contains(tuple1));
		Assert.assertTrue(resultTupleList.contains(tuple2));
		Assert.assertTrue(resultTupleList.contains(tuple3));
	}

	/**
	 * Simple BBox query - across multiple tables on disk - after compact
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws RejectedException
	 * @throws IOException
	 */
	@Test(timeout=60000)
	public void testBBoxQuery4() throws StorageManagerException, InterruptedException, RejectedException, IOException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.flush();

		storageManager.put(tuple2);
		storageManager.flush();

		storageManager.put(tuple3);
		storageManager.flush();

		final Hyperrectangle queryBoundingBox = new Hyperrectangle(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<MultiTuple> iterator = queryPlan.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);
		final List<Tuple> resultTupleList = resultList.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultTupleList.contains(tuple1));
		Assert.assertTrue(resultTupleList.contains(tuple2));
		Assert.assertTrue(resultTupleList.contains(tuple3));
	}

	/**
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException
	 */
	@Test(timeout=60000)
	public void testJoin1() throws StorageManagerException, RejectedException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_2);
		storageRegistry.createTable(TABLE_2, new TupleStoreConfiguration());

		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final Hyperrectangle queryRange = Hyperrectangle.FULL_SPACE;
		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, queryRange);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, queryRange);

		final IndexedSpatialJoinOperator joinQueryProcessor = new IndexedSpatialJoinOperator(operator1,
				operator2);

		final Iterator<MultiTuple> iterator = joinQueryProcessor.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);
		joinQueryProcessor.close();

		Assert.assertEquals(0, resultList.size());
	}

	/**
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException
	 */
	@Test(timeout=60000)
	public void testJoin2() throws StorageManagerException, RejectedException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_2);
		storageRegistry.createTable(TABLE_2, new TupleStoreConfiguration());

		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final Tuple tuple1 = new Tuple("1a", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new Hyperrectangle(4.0, 5.0, 4.0, 5.0), "value2".getBytes());

		final Tuple tuple3 = new Tuple("1b", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("2b", new Hyperrectangle(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);

		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);

		final Hyperrectangle queryBox = new Hyperrectangle(3.0, 10.0, 3.0, 10.0);
		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, queryBox);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, queryBox);

		// Operator 1 // Operator 2
		final IndexedSpatialJoinOperator joinQueryProcessor1 = new IndexedSpatialJoinOperator(operator1,
				operator2);

		final Iterator<MultiTuple> iterator1 = joinQueryProcessor1.iterator();

		final List<MultiTuple> resultList1 = Lists.newArrayList(iterator1);
		joinQueryProcessor1.close();

		Assert.assertEquals(1, resultList1.size());
		Assert.assertEquals(2, resultList1.get(0).getNumberOfTuples());
		Assert.assertEquals(2, resultList1.get(0).getBoundingBox().getDimension());
		Assert.assertEquals(new Hyperrectangle(2.5d, 5.5d, 2.5d, 5.5d), resultList1.get(0).getBoundingBox());
	
		// Operator 2 // Operator 1
		final IndexedSpatialJoinOperator joinQueryProcessor2 = new IndexedSpatialJoinOperator(operator2,
				operator1);

		final Iterator<MultiTuple> iterator2 = joinQueryProcessor2.iterator();

		final List<MultiTuple> resultList2 = Lists.newArrayList(iterator2);
		joinQueryProcessor2.close();

		Assert.assertEquals(1, resultList2.size());
		Assert.assertEquals(2, resultList2.get(0).getNumberOfTuples());
		Assert.assertEquals(2, resultList2.get(0).getBoundingBox().getDimension());
		Assert.assertEquals(new Hyperrectangle(2.5d, 5.5d, 2.5d, 5.5d), resultList2.get(0).getBoundingBox());		
	}
	
	/**
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException
	 */
	@Test(timeout=60000)
	public void testJoin3() throws StorageManagerException, RejectedException {
		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_2);
		storageRegistry.createTable(TABLE_2, new TupleStoreConfiguration());

		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final Tuple tuple1 = new Tuple("1a", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new Hyperrectangle(4.0, 5.0, 4.0, 5.0), "value2".getBytes());
		final Tuple tuple3 = new Tuple("3a", new Hyperrectangle(-10.0, 5.0, 0.0, 5.0), "value3".getBytes());
		final Tuple tuple4 = new Tuple("4a", new Hyperrectangle(-4.0, 5.0, 24.0, 55.0), "value4".getBytes());
		final Tuple tuple5 = new Tuple("5a", new Hyperrectangle(-40.0, 5.0, 40.0, 55.0), "value5".getBytes());

		final Tuple tuple6 = new Tuple("1b", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value6".getBytes());
		final Tuple tuple7 = new Tuple("2b", new Hyperrectangle(2.5, 5.5, 2.5, 5.5), "value7".getBytes());
		final Tuple tuple8 = new Tuple("3b", new Hyperrectangle(2.5, 50.5, 20.5, 75.5), "value8".getBytes());
		final Tuple tuple9 = new Tuple("4b", new Hyperrectangle(20.5, 25.5, 20.5, 115.5), "value9".getBytes());
		final Tuple tuple10 = new Tuple("5b", new Hyperrectangle(-2.5, 5.5, 20.5, 125.5), "value10".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);
		storageManager1.put(tuple3);
		storageManager1.put(tuple4);
		storageManager1.put(tuple5);

		// Table2
		storageManager2.put(tuple6);
		storageManager2.put(tuple7);
		storageManager2.put(tuple8);
		storageManager2.put(tuple9);
		storageManager2.put(tuple10);

		final Hyperrectangle queryBox = new Hyperrectangle(3.0, 10.0, 3.0, 10.0);
 
		// Join 1: Operator 1 // Operator 2
		final SpatialIndexReadOperator operator11 = new SpatialIndexReadOperator(storageManager1, queryBox);
		final SpatialIndexReadOperator operator12 = new SpatialIndexReadOperator(storageManager2, queryBox);

		final IndexedSpatialJoinOperator joinQueryProcessor1 = new IndexedSpatialJoinOperator(operator11,
				operator12);

		final Iterator<MultiTuple> iterator1 = joinQueryProcessor1.iterator();

		final List<MultiTuple> resultList1 = Lists.newArrayList(iterator1);
		joinQueryProcessor1.close();
		
		// Join 2: Operator 2 // Operator 1
		final SpatialIndexReadOperator operator21 = new SpatialIndexReadOperator(storageManager1, queryBox);
		final SpatialIndexReadOperator operator22 = new SpatialIndexReadOperator(storageManager2, queryBox);

		final IndexedSpatialJoinOperator joinQueryProcessor2 = new IndexedSpatialJoinOperator(operator22,
				operator21);

		final Iterator<MultiTuple> iterator2 = joinQueryProcessor2.iterator();

		final List<MultiTuple> resultList2 = Lists.newArrayList(iterator2);
		joinQueryProcessor2.close();
		
		Assert.assertEquals(resultList1.size(), resultList2.size());
		Assert.assertEquals(2, resultList1.size());
		
		// Join 2: Operator 1 // Operator 2
		final Hyperrectangle queryBox2 = new Hyperrectangle(-40.0, -30.0, 40.0, 55.0);
		final SpatialIndexReadOperator operator31 = new SpatialIndexReadOperator(storageManager1, queryBox2);
		final SpatialIndexReadOperator operator32 = new SpatialIndexReadOperator(storageManager2, queryBox2);

		final IndexedSpatialJoinOperator joinQueryProcessor3 = new IndexedSpatialJoinOperator(operator31,
				operator32);

		final Iterator<MultiTuple> iterator3 = joinQueryProcessor3.iterator();

		final List<MultiTuple> resultList3 = Lists.newArrayList(iterator3);
		joinQueryProcessor3.close();
		
		//System.out.println("---> " + resultList3.get(0).getTuple(0).getBoundingBox().toCompactString());
		//System.out.println("---> " + resultList3.get(0).getTuple(1).getBoundingBox().toCompactString());
		
		Assert.assertTrue(resultList3.isEmpty());
		
		// Join 2: Operator 2 // Operator 1
		final SpatialIndexReadOperator operator41 = new SpatialIndexReadOperator(storageManager1, queryBox2);
		final SpatialIndexReadOperator operator42 = new SpatialIndexReadOperator(storageManager2, queryBox2);

		final IndexedSpatialJoinOperator joinQueryProcessor4 = new IndexedSpatialJoinOperator(operator42,
				operator41);

		final Iterator<MultiTuple> iterator4 = joinQueryProcessor4.iterator();

		final List<MultiTuple> resultList4 = Lists.newArrayList(iterator4);
		joinQueryProcessor4.close();
		
		//System.out.println("---> " + resultList4.get(0).getTuple(0).getBoundingBox().toCompactString());
		//System.out.println("---> " + resultList4.get(0).getTuple(1).getBoundingBox().toCompactString());
		
		Assert.assertTrue(resultList4.isEmpty());
	}

	/**
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException
	 */
	@Test(timeout=60000)
	public void testDoubleJoin1() throws StorageManagerException, RejectedException {

		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_2);
		storageRegistry.createTable(TABLE_2, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_3);
		storageRegistry.createTable(TABLE_3, new TupleStoreConfiguration());

		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);
		final TupleStoreManager storageManager3 = storageRegistry.getTupleStoreManager(TABLE_3);

		final Tuple tuple1 = new Tuple("1a", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new Hyperrectangle(4.0, 5.0, 4.0, 5.0), "value2".getBytes());

		final Tuple tuple3 = new Tuple("1b", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("2b", new Hyperrectangle(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		final Tuple tuple5 = new Tuple("1c", new Hyperrectangle(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);

		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);

		// Table3
		storageManager3.put(tuple5);

		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, Hyperrectangle.FULL_SPACE);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, Hyperrectangle.FULL_SPACE);
		final SpatialIndexReadOperator operator3 = new SpatialIndexReadOperator(storageManager3, Hyperrectangle.FULL_SPACE);

		final IndexedSpatialJoinOperator joinQueryProcessor1 = new IndexedSpatialJoinOperator(operator1,
				operator2);

		final IndexedSpatialJoinOperator joinQueryProcessor2 = new IndexedSpatialJoinOperator(joinQueryProcessor1,
				operator3);

		final Iterator<MultiTuple> iterator = joinQueryProcessor2.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);

		joinQueryProcessor2.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertEquals(3, resultList.get(0).getNumberOfTuples());

		Assert.assertEquals(2, resultList.get(0).getBoundingBox().getDimension());

		Assert.assertEquals(new Hyperrectangle(1.0d, 5.5d, 1.0d, 5.5d), resultList.get(0).getBoundingBox());
	}

	/**
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException
	 */
	@Test(timeout=60000)
	public void testJoinWithChangedTuple1() throws StorageManagerException, RejectedException {

		storageRegistry.deleteTable(TABLE_1);
		storageRegistry.createTable(TABLE_1, new TupleStoreConfiguration());

		storageRegistry.deleteTable(TABLE_2);
		storageRegistry.createTable(TABLE_2, new TupleStoreConfiguration());

		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final Tuple tuple1 = new Tuple("1a", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new Hyperrectangle(4.0, 5.0, 4.0, 5.0), "value2".getBytes());

		// Tuple 3 and tuple 4 have the same key
		final Tuple tuple3 = new Tuple("1b", new Hyperrectangle(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("1b", new Hyperrectangle(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);

		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);

		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, Hyperrectangle.FULL_SPACE);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, Hyperrectangle.FULL_SPACE);

		final IndexedSpatialJoinOperator joinQueryProcessor1 = new IndexedSpatialJoinOperator(operator1,
				operator2);

		final Iterator<MultiTuple> iterator = joinQueryProcessor1.iterator();

		final List<MultiTuple> resultList = Lists.newArrayList(iterator);

		joinQueryProcessor1.close();

		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals(2, resultList.get(0).getNumberOfTuples());
		Assert.assertEquals(2, resultList.get(0).getBoundingBox().getDimension());
		Assert.assertEquals(new Hyperrectangle(2.5d, 5.5d, 2.5d, 5.5d), resultList.get(0).getBoundingBox());
	}
}

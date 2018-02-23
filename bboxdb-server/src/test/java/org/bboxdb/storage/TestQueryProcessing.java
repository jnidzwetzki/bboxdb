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
package org.bboxdb.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.operator.BoundingBoxSelectOperator;
import org.bboxdb.storage.queryprocessor.operator.FullTablescanOperator;
import org.bboxdb.storage.queryprocessor.operator.IndexedSpatialJoinOperator;
import org.bboxdb.storage.queryprocessor.operator.Operator;
import org.bboxdb.storage.queryprocessor.operator.SpatialIndexReadOperator;
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
	protected static final TupleStoreName TABLE_1 = new TupleStoreName("junitgroup_table1");
	
	/**
	 * The 2. table name for tests
	 */
	protected static final TupleStoreName TABLE_2 = new TupleStoreName("junitgroup_table2");

	/**
	 * The 3. table name for tests
	 */
	protected static final TupleStoreName TABLE_3 = new TupleStoreName("junitgroup_table3");

	/**
	 * The storage registry
	 */
	protected static TupleStoreManagerRegistry storageRegistry;
	
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
	@Test
	public void testBBoxQuery1() throws StorageManagerException, RejectedException, IOException {
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		storageManager.put(tuple3);
		
		final BoundingBox queryBoundingBox = new BoundingBox(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<JoinedTuple> iterator = queryPlan.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
		final List<Tuple> resultTupleList = resultList.stream().map(t -> t.convertToSingleTupleIfPossible()).collect(Collectors.toList());
		queryPlan.close();

		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultTupleList.contains(tuple1));
		Assert.assertTrue(resultTupleList.contains(tuple2));
		Assert.assertTrue(resultTupleList.contains(tuple3));
		
		// Reopen 
		final Iterator<JoinedTuple> iterator2 = queryPlan.iterator();
		
		final List<JoinedTuple> resultList2 = Lists.newArrayList(iterator2);
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
	@Test
	public void testBBoxQuery2() throws StorageManagerException, RejectedException, IOException {
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.initNewMemtable();
		storageManager.put(tuple2);
		storageManager.initNewMemtable();
		storageManager.put(tuple3);
		storageManager.initNewMemtable();
		
		final BoundingBox queryBoundingBox = new BoundingBox(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<JoinedTuple> iterator = queryPlan.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
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
	@Test
	public void testBBoxQuery3() throws StorageManagerException, InterruptedException, RejectedException, IOException {
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.flush();
		
		storageManager.put(tuple2);
		storageManager.flush();
		
		storageManager.put(tuple3);
		storageManager.flush();
		
		final BoundingBox queryBoundingBox = new BoundingBox(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<JoinedTuple> iterator = queryPlan.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
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
	@Test
	public void testBBoxQuery4() throws StorageManagerException, InterruptedException, RejectedException, IOException {
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		
		storageManager.put(tuple1);
		storageManager.flush();
		
		storageManager.put(tuple2);
		storageManager.flush();
		
		storageManager.put(tuple3);
		storageManager.flush();
		
		final BoundingBox queryBoundingBox = new BoundingBox(0.0, 5.0, 0.0, 5.0);
		final Operator spatialIndexReadOperator = new FullTablescanOperator(storageManager);
		final Operator queryPlan = new BoundingBoxSelectOperator(queryBoundingBox, spatialIndexReadOperator);

		final Iterator<JoinedTuple> iterator = queryPlan.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
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
	@Test
	public void testJoin1() throws StorageManagerException, RejectedException {
		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, BoundingBox.FULL_SPACE);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, BoundingBox.FULL_SPACE);
		
		final IndexedSpatialJoinOperator joinQueryProcessor = new IndexedSpatialJoinOperator(operator1, 
				operator2);
		
		final Iterator<JoinedTuple> iterator = joinQueryProcessor.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
		joinQueryProcessor.close();

		Assert.assertEquals(0, resultList.size());
	}
	
	/** 
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	@Test
	public void testJoin3() throws StorageManagerException, RejectedException {
		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final Tuple tuple1 = new Tuple("1a", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new BoundingBox(4.0, 5.0, 4.0, 5.0), "value2".getBytes());
		
		final Tuple tuple3 = new Tuple("1b", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("2b", new BoundingBox(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);
		
		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);
		
		final BoundingBox queryBox = new BoundingBox(3.0, 10.0, 3.0, 10.0);
		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, queryBox);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, queryBox);
		
		final IndexedSpatialJoinOperator joinQueryProcessor = new IndexedSpatialJoinOperator(operator1, 
				operator2);
		
		final Iterator<JoinedTuple> iterator = joinQueryProcessor.iterator();
	
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
		joinQueryProcessor.close();

		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals(2, resultList.get(0).getNumberOfTuples());
		Assert.assertEquals(2, resultList.get(0).getBoundingBox().getDimension());
		Assert.assertEquals(new BoundingBox(4.0d, 5.0d, 4.0d, 5.0d), resultList.get(0).getBoundingBox());
	}
	
	/** 
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	@Test
	public void testDoubleJoin1() throws StorageManagerException, RejectedException {
		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);
		final TupleStoreManager storageManager3 = storageRegistry.getTupleStoreManager(TABLE_3);

		final Tuple tuple1 = new Tuple("1a", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new BoundingBox(4.0, 5.0, 4.0, 5.0), "value2".getBytes());
		
		final Tuple tuple3 = new Tuple("1b", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("2b", new BoundingBox(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		final Tuple tuple5 = new Tuple("1c", new BoundingBox(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);
		
		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);
		
		// Table3
		storageManager3.put(tuple5);
		
		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, BoundingBox.FULL_SPACE);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, BoundingBox.FULL_SPACE);
		final SpatialIndexReadOperator operator3 = new SpatialIndexReadOperator(storageManager3, BoundingBox.FULL_SPACE);

		final IndexedSpatialJoinOperator joinQueryProcessor1 = new IndexedSpatialJoinOperator(operator1, 
				operator2);
		
		final IndexedSpatialJoinOperator joinQueryProcessor2 = new IndexedSpatialJoinOperator(joinQueryProcessor1, 
				operator3);
		
		final Iterator<JoinedTuple> iterator = joinQueryProcessor2.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
			
		joinQueryProcessor2.close();
		
		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals(3, resultList.get(0).getNumberOfTuples());
		
		Assert.assertEquals(2, resultList.get(0).getBoundingBox().getDimension());
		
		Assert.assertEquals(new BoundingBox(4.0d, 5.0d, 4.0d, 5.0d), resultList.get(0).getBoundingBox());
	}	
	
	/** 
	 * Simple Join
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	@Test
	public void testJoinWithChangedTuple1() throws StorageManagerException, RejectedException {
		final TupleStoreManager storageManager1 = storageRegistry.getTupleStoreManager(TABLE_1);
		final TupleStoreManager storageManager2 = storageRegistry.getTupleStoreManager(TABLE_2);

		final Tuple tuple1 = new Tuple("1a", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new BoundingBox(4.0, 5.0, 4.0, 5.0), "value2".getBytes());
		
		// Tuple 3 and tuple 4 have the same key
		final Tuple tuple3 = new Tuple("1b", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("1b", new BoundingBox(2.5, 5.5, 2.5, 5.5), "value4".getBytes());
		
		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);
		
		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);
		
		final SpatialIndexReadOperator operator1 = new SpatialIndexReadOperator(storageManager1, BoundingBox.FULL_SPACE);
		final SpatialIndexReadOperator operator2 = new SpatialIndexReadOperator(storageManager2, BoundingBox.FULL_SPACE);

		final IndexedSpatialJoinOperator joinQueryProcessor1 = new IndexedSpatialJoinOperator(operator1, 
				operator2);

		final Iterator<JoinedTuple> iterator = joinQueryProcessor1.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
			
		joinQueryProcessor1.close();
		
		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals(2, resultList.get(0).getNumberOfTuples());
		Assert.assertEquals(2, resultList.get(0).getBoundingBox().getDimension());
		Assert.assertEquals(new BoundingBox(4.0d, 5.0d, 4.0d, 5.0d), resultList.get(0).getBoundingBox());
	}
}

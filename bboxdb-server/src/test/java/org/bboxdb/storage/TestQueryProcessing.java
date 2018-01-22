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

import java.util.List;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.JoinQueryProcessor;
import org.bboxdb.storage.queryprocessor.SelectionQueryProcessor;
import org.bboxdb.storage.queryprocessor.queryplan.BoundingBoxQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.util.CloseableIterator;
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
	protected static final TupleStoreName TABLE_1 = new TupleStoreName("2_junitgroup_table1");
	
	/**
	 * The 2. table name for tests
	 */
	protected static final TupleStoreName TABLE_2 = new TupleStoreName("2_junitgroup_table2");

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
	}
	
	/** 
	 * Simple BBox query
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	@Test
	public void testBBoxQuery1() throws StorageManagerException, RejectedException {
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TABLE_1);

		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		storageManager.put(tuple3);
		
		final BoundingBox queryBoundingBox = new BoundingBox(0.0, 5.0, 0.0, 5.0);
		final QueryPlan queryPlan = new BoundingBoxQueryPlan(queryBoundingBox);

		final SelectionQueryProcessor queryProcessor = new SelectionQueryProcessor(queryPlan, storageManager);
		final CloseableIterator<Tuple> iterator = queryProcessor.iterator();
		
		final List<Tuple> resultList = Lists.newArrayList(iterator);
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
	}
	
	/** 
	 * Simple BBox query - across multiple tables
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	@Test
	public void testBBoxQuery2() throws StorageManagerException, RejectedException {
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
		final QueryPlan queryPlan = new BoundingBoxQueryPlan(queryBoundingBox);

		final SelectionQueryProcessor queryProcessor = new SelectionQueryProcessor(queryPlan, storageManager);
		final CloseableIterator<Tuple> iterator = queryProcessor.iterator();
		
		final List<Tuple> resultList = Lists.newArrayList(iterator);
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
	}
	
	/** 
	 * Simple BBox query - across multiple tables on disk
	 * @throws StorageManagerException
	 * @throws InterruptedException 
	 * @throws RejectedException 
	 */
	@Test
	public void testBBoxQuery3() throws StorageManagerException, InterruptedException, RejectedException {
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
		final QueryPlan queryPlan = new BoundingBoxQueryPlan(queryBoundingBox);
		
		final SelectionQueryProcessor queryProcessor = new SelectionQueryProcessor(queryPlan, storageManager);
		final CloseableIterator<Tuple> iterator = queryProcessor.iterator();
		
		final List<Tuple> resultList = Lists.newArrayList(iterator);
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
	}
	
	/** 
	 * Simple BBox query - across multiple tables on disk - after compact
	 * @throws StorageManagerException
	 * @throws InterruptedException 
	 * @throws RejectedException 
	 */
	@Test
	public void testBBoxQuery4() throws StorageManagerException, InterruptedException, RejectedException {
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
		final QueryPlan queryPlan = new BoundingBoxQueryPlan(queryBoundingBox);
		
		final SelectionQueryProcessor queryProcessor = new SelectionQueryProcessor(queryPlan, storageManager);
		final CloseableIterator<Tuple> iterator = queryProcessor.iterator();
		
		final List<Tuple> resultList = Lists.newArrayList(iterator);
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
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

		final Tuple tuple1 = new Tuple("1a", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());
		final Tuple tuple2 = new Tuple("2a", new BoundingBox(4.0, 5.0, 4.0, 5.0), "value2".getBytes());
		
		final Tuple tuple3 = new Tuple("1b", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value3".getBytes());
		final Tuple tuple4 = new Tuple("1b", new BoundingBox(2.5, 5.5, 2.5, 5.5), "value4".getBytes());

		// Table1
		storageManager1.put(tuple1);
		storageManager1.put(tuple2);
		
		// Table2
		storageManager2.put(tuple3);
		storageManager2.put(tuple4);
		
		final JoinQueryProcessor joinQueryProcessor = new JoinQueryProcessor(storageManager1, 
				storageManager2, BoundingBox.EMPTY_BOX);
		
		final CloseableIterator<JoinedTuple> iterator = joinQueryProcessor.iterator();
		
		final List<JoinedTuple> resultList = Lists.newArrayList(iterator);
		
		Assert.assertEquals(2, resultList.size());
	}
}

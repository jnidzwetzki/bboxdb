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
package org.bboxdb.storage;

import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.CloseableIterator;
import org.bboxdb.storage.queryprocessor.QueryProcessor;
import org.bboxdb.storage.queryprocessor.queryplan.BoundingBoxQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.registry.StorageRegistry;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.util.RejectedException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;


public class TestQueryProcessing {

	/**
	 * The table name for tests
	 */
	protected static final SSTableName TABLE = new SSTableName("2_junitgroup_table1");
	
	/** 
	 * Simple BBox query
	 * @throws StorageManagerException
	 * @throws RejectedException 
	 */
	@Test
	public void testBBoxQuery1() throws StorageManagerException, RejectedException {

		StorageRegistry.getInstance().deleteTable(TABLE);
		final SSTableManager storageManager = StorageRegistry.getInstance().getSSTableManager(TABLE);

		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value".getBytes());
		final Tuple tuple2 = new Tuple("2", new BoundingBox(1.5, 2.5, 1.5, 2.5), "value2".getBytes());
		final Tuple tuple3 = new Tuple("1", new BoundingBox(1.0, 2.0, 1.0, 2.0), "value1".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		storageManager.put(tuple3);
		
		final BoundingBox queryBoundingBox = new BoundingBox(0.0, 5.0, 0.0, 5.0);
		final QueryPlan queryPlan = new BoundingBoxQueryPlan(queryBoundingBox);

		final QueryProcessor queryProcessor = new QueryProcessor(queryPlan, storageManager);
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
		
		StorageRegistry.getInstance().deleteTable(TABLE);
		final SSTableManager storageManager = StorageRegistry.getInstance().getSSTableManager(TABLE);

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

		final QueryProcessor queryProcessor = new QueryProcessor(queryPlan, storageManager);
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
		StorageRegistry.getInstance().deleteTable(TABLE);
		final SSTableManager storageManager = StorageRegistry.getInstance().getSSTableManager(TABLE);

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
		
		final QueryProcessor queryProcessor = new QueryProcessor(queryPlan, storageManager);
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
		StorageRegistry.getInstance().deleteTable(TABLE);
		final SSTableManager storageManager = StorageRegistry.getInstance().getSSTableManager(TABLE);

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
		
		final QueryProcessor queryProcessor = new QueryProcessor(queryPlan, storageManager);
		final CloseableIterator<Tuple> iterator = queryProcessor.iterator();
		
		final List<Tuple> resultList = Lists.newArrayList(iterator);
		
		Assert.assertEquals(2, resultList.size());
		Assert.assertFalse(resultList.contains(tuple1));
		Assert.assertTrue(resultList.contains(tuple2));
		Assert.assertTrue(resultList.contains(tuple3));
	}	

}

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

import java.util.concurrent.TimeUnit;

import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.SSTableManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTableCheckpoint {
	
	/**
	 * The name of the test relation
	 */
	protected final static SSTableName TEST_RELATION = new SSTableName("1_testgroup1_abc");
	
	/**
	 * Ensure that the distribution group is recreated
	 * @throws ZookeeperException
	 */
	@Before
	public void before() throws ZookeeperException {
		final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_RELATION.getDistributionGroup());
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_RELATION.getDistributionGroup(), (short) 1);
	}
	
	/**
	 * Test insert without flush thread
	 * @throws StorageManagerException
	 */
	@Test
	public void testInsertWithoutFlush() throws StorageManagerException {
		
		// Prepare sstable manager
		StorageRegistry.shutdown(TEST_RELATION);
		BBoxDBConfigurationManager.getConfiguration().setStorageCheckpointInterval(0);
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(TEST_RELATION);
		storageManager.clear();

		Assert.assertTrue(storageManager.getMemtable().isEmpty());
		final Tuple tuple = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes());
		storageManager.put(tuple);
		Assert.assertFalse(storageManager.getMemtable().isEmpty());
		
		Assert.assertEquals(tuple, storageManager.get("1"));
	}
	
	/**
	 * Test insert with flush
	 * @throws StorageManagerException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testInsertWithFlush() throws StorageManagerException, InterruptedException {
		
		final int CHECKPOINT_INTERVAL = 10;

		// Prepare sstable manager
		StorageRegistry.shutdown(TEST_RELATION);
		BBoxDBConfigurationManager.getConfiguration().setStorageCheckpointInterval(CHECKPOINT_INTERVAL);
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(TEST_RELATION);
		storageManager.clear();
		
		Assert.assertTrue(storageManager.getMemtable().isEmpty());
		final Tuple tuple = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes());
		storageManager.put(tuple);
		Assert.assertFalse(storageManager.getMemtable().isEmpty());

		// Wait for checkpoint
		Thread.sleep(SSTableConst.CHECKPOINT_THREAD_DELAY + TimeUnit.SECONDS.toMillis(10));
		
		Assert.assertTrue(storageManager.getMemtable().isEmpty());
		
		Assert.assertEquals(tuple, storageManager.get("1"));
	}

}

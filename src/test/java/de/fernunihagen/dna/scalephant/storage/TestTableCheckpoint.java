/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.storage;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableConst;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

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
		ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
		zookeeperClient.deleteDistributionGroup(TEST_RELATION.getDistributionGroup());
		zookeeperClient.createDistributionGroup(TEST_RELATION.getDistributionGroup(), (short) 1);
	}
	
	/**
	 * Test insert without flush thread
	 * @throws StorageManagerException
	 */
	@Test
	public void testInsertWithoutFlush() throws StorageManagerException {
		
		// Prepare sstable manager
		StorageRegistry.shutdown(TEST_RELATION);
		ScalephantConfigurationManager.getConfiguration().setStorageCheckpointInterval(0);
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
		ScalephantConfigurationManager.getConfiguration().setStorageCheckpointInterval(CHECKPOINT_INTERVAL);
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

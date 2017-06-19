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

import java.io.File;
import java.util.List;

import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.registry.StorageRegistry;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.util.RejectedException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStorageRegistry {
	
	/**
	 * The name of the test relation
	 */
	protected static final SSTableName RELATION_NAME = new SSTableName("3_grouptest1_table1_2");

	/**
	 * The storage registry
	 */
	protected static StorageRegistry storageRegistry;
	
	@BeforeClass
	public static void beforeClass() throws InterruptedException, BBoxDBException {
		storageRegistry = new StorageRegistry();
		storageRegistry.init();
	}
	
	@AfterClass
	public static void afterClass() {
		if(storageRegistry != null) {
			storageRegistry.shutdown();
			storageRegistry = null;
		}
	}
	
	/**
	 * Test registering and unregistering the storage manager
	 * @throws StorageManagerException 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRegisterAndUnregister() throws StorageManagerException {
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.getSSTableManager(RELATION_NAME);
		Assert.assertTrue(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.shutdownSStable(RELATION_NAME);
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
	}
	
	/**
	 * Test delete table
	 * @throws StorageManagerException 
	 * @throws InterruptedException 
	 * @throws RejectedException 
	 */
	@Test
	public void testDeleteTable() throws StorageManagerException, InterruptedException, RejectedException {
		
		final SSTableManager storageManager = storageRegistry.getSSTableManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		Thread.sleep(10000);
		
		storageRegistry.deleteTable(RELATION_NAME);
		
		Assert.assertTrue(storageManager.isShutdownComplete());
		
		// Check the removal of the directory
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		final String storageDirectory = configuration.getStorageDirectories().get(0);

		final String pathname = SSTableHelper.getSSTableDir(storageDirectory, RELATION_NAME);
		final File directory = new File(pathname);
		
		System.out.println("Check for " + directory);
		
		Assert.assertFalse(directory.exists());
	}
	
	/**
	 * Calculate the size of a distribution group
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws RejectedException 
	 */
	@Test
	public void testCalculateSize() throws StorageManagerException, InterruptedException, RejectedException {
		
		final SSTableManager storageManager = storageRegistry.getSSTableManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		Thread.sleep(10000);
		
		final List<SSTableName> tablesBeforeDelete = storageRegistry.getAllTables();
		System.out.println(tablesBeforeDelete);
		Assert.assertTrue(tablesBeforeDelete.contains(RELATION_NAME));
		
		final long size1 = storageRegistry.getSizeOfDistributionGroupAndRegionId(
						RELATION_NAME.getDistributionGroupObject(), 2);
		
		Assert.assertTrue(size1 > 0);
		
		storageRegistry.deleteTable(RELATION_NAME);
		
		final List<SSTableName> tablesAfterDelete = storageRegistry.getAllTables();
		System.out.println(tablesAfterDelete);
		Assert.assertFalse(tablesAfterDelete.contains(RELATION_NAME));
		
		final long size2 = storageRegistry.getSizeOfDistributionGroupAndRegionId(
						RELATION_NAME.getDistributionGroupObject(), 2);
		
		Assert.assertTrue(size2 == 0);
	}
	
}

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

import java.io.File;
import java.util.List;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStorageRegistry {
	
	/**
	 * The name of the test relation
	 */
	protected static final TupleStoreName RELATION_NAME = new TupleStoreName("grouptest5_table100_2");

	/**
	 * The storage registry
	 */
	protected static TupleStoreManagerRegistry storageRegistry;
	
	@BeforeClass
	public static void beforeClass() throws InterruptedException, BBoxDBException, ZookeeperException {
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		final DistributionGroupZookeeperAdapter adapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
		adapter.createDistributionGroup(RELATION_NAME.getDistributionGroup(), new DistributionGroupConfiguration());
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
		storageRegistry.deleteTable(RELATION_NAME);
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.createTable(RELATION_NAME, new TupleStoreConfiguration());
		storageRegistry.getTupleStoreManager(RELATION_NAME);
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
		storageRegistry.deleteTable(RELATION_NAME);
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.createTable(RELATION_NAME, new TupleStoreConfiguration());
		
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, Integer.toString(i).getBytes());
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
		
		storageRegistry.deleteTable(RELATION_NAME);
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.createTable(RELATION_NAME, new TupleStoreConfiguration());
		
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		Thread.sleep(10000);
		
		final List<TupleStoreName> tablesBeforeDelete = storageRegistry.getAllTables();
		System.out.println(tablesBeforeDelete);
		Assert.assertTrue(tablesBeforeDelete.contains(RELATION_NAME));
		
		final long size1 = storageRegistry.getSizeOfDistributionGroupAndRegionId(
						RELATION_NAME.getDistributionGroupObject(), 2);
		
		Assert.assertTrue(size1 > 0);
		
		storageRegistry.deleteTable(RELATION_NAME);
		
		final List<TupleStoreName> tablesAfterDelete = storageRegistry.getAllTables();
		System.out.println(tablesAfterDelete);
		Assert.assertFalse(tablesAfterDelete.contains(RELATION_NAME));
		
		final long size2 = storageRegistry.getSizeOfDistributionGroupAndRegionId(
						RELATION_NAME.getDistributionGroupObject(), 2);
		
		Assert.assertTrue(size2 == 0);
	}
	
}

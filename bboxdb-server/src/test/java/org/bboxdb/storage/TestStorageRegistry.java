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
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.tuplestore.manager.TupleStoreUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStorageRegistry {
	
	/**
	 * The name of the test relation
	 */
	private static final TupleStoreName RELATION_NAME = new TupleStoreName("grouptest5_table100_2");

	/**
	 * The storage registry
	 */
	private static TupleStoreManagerRegistry storageRegistry;
	
	@BeforeClass
	public static void beforeClass() throws InterruptedException, BBoxDBException, ZookeeperException {
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		final DistributionGroupAdapter adapter = new DistributionGroupAdapter(zookeeperClient);
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		adapter.createDistributionGroup(RELATION_NAME.getDistributionGroup(), configuration);
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
	@Test(timeout=60000)
	public void testRegisterAndUnregister() throws StorageManagerException {
		storageRegistry.deleteTable(RELATION_NAME, true);
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
	@Test(timeout=60000)
	public void testDeleteTable() throws StorageManagerException, InterruptedException, RejectedException {
		storageRegistry.deleteTable(RELATION_NAME, true);
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.createTable(RELATION_NAME, new TupleStoreConfiguration());
		
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), Hyperrectangle.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		storageManager.flush();
		
		storageRegistry.deleteTable(RELATION_NAME, true);
		
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
	@Test(timeout=60000)
	public void testCalculateSize() throws StorageManagerException, InterruptedException, RejectedException {
		
		storageRegistry.deleteTable(RELATION_NAME, true);
		Assert.assertFalse(storageRegistry.isStorageManagerActive(RELATION_NAME));
		storageRegistry.createTable(RELATION_NAME, new TupleStoreConfiguration());
		
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), Hyperrectangle.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		storageManager.flush();
		
		final List<TupleStoreName> tablesBeforeDelete = storageRegistry.getAllTables();
		System.out.println(tablesBeforeDelete);
		Assert.assertTrue(tablesBeforeDelete.contains(RELATION_NAME));
				
		final long size1 = TupleStoreUtil.getSizeOfDistributionGroupAndRegionId(
				storageRegistry, RELATION_NAME.getDistributionGroup(), 2);
		
		Assert.assertTrue(size1 > 0);
		
		storageRegistry.deleteTable(RELATION_NAME, true);
		
		final List<TupleStoreName> tablesAfterDelete = storageRegistry.getAllTables();
		System.out.println(tablesAfterDelete);
		Assert.assertFalse(tablesAfterDelete.contains(RELATION_NAME));
		
		final long size2 = TupleStoreUtil.getSizeOfDistributionGroupAndRegionId(
				storageRegistry, RELATION_NAME.getDistributionGroup(), 2);
		
		Assert.assertTrue(size2 == 0);
	}
	
}

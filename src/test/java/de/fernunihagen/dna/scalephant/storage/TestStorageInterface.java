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

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

public class TestStorageInterface {
	
	/**
	 * The name of the test relation
	 */
	protected static final SSTableName RELATION_NAME = new SSTableName("3_grouptest1_table1");

	/**
	 * Test registering and unregistering the storage manager
	 * @throws StorageManagerException 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRegisterAndUnregister() throws StorageManagerException {
		Assert.assertFalse(StorageRegistry.isStorageManagerActive(RELATION_NAME));
		StorageRegistry.getSSTableManager(RELATION_NAME);
		Assert.assertTrue(StorageRegistry.isStorageManagerActive(RELATION_NAME));
		StorageRegistry.shutdown(RELATION_NAME);
		Assert.assertFalse(StorageRegistry.isStorageManagerActive(RELATION_NAME));
	}
	
	/**
	 * Test delete table
	 * @throws StorageManagerException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testDeleteTable() throws StorageManagerException, InterruptedException {
		
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		Thread.sleep(10000);
		
		StorageRegistry.deleteTable(RELATION_NAME);
		
		Assert.assertTrue(storageManager.isShutdownComplete());
		
		// Check the removal of the directory
		final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();
		final String pathname = configuration.getDataDirectory() + File.separator + RELATION_NAME;
		final File directory = new File(pathname);
		Assert.assertFalse(directory.exists());
	}
}

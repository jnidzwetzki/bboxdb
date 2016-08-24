package de.fernunihagen.dna.scalephant.storage;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.storage.StorageInterface;
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
		Assert.assertFalse(StorageInterface.isStorageManagerActive(RELATION_NAME));
		StorageInterface.getSSTableManager(RELATION_NAME);
		Assert.assertTrue(StorageInterface.isStorageManagerActive(RELATION_NAME));
		StorageInterface.shutdown(RELATION_NAME);
		Assert.assertFalse(StorageInterface.isStorageManagerActive(RELATION_NAME));
	}
	
	/**
	 * Test delete table
	 * @throws StorageManagerException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testDeleteTable() throws StorageManagerException, InterruptedException {
		
		final SSTableManager storageManager = StorageInterface.getSSTableManager(RELATION_NAME);
		
		for(int i = 0; i < 50000; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		// Wait for requests to settle
		Thread.sleep(10000);
		
		StorageInterface.deleteTable(RELATION_NAME);
		
		Assert.assertTrue(storageManager.isShutdownComplete());
		
		// Check the removal of the directory
		final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();
		final String pathname = configuration.getDataDirectory() + File.separator + RELATION_NAME;
		final File directory = new File(pathname);
		Assert.assertFalse(directory.exists());
	}
}

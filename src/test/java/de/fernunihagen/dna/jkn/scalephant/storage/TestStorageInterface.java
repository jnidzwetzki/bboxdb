package de.fernunihagen.dna.jkn.scalephant.storage;

import junit.framework.Assert;

import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public class TestStorageInterface {
	
	/**
	 * The name of the test relation
	 */
	protected static final String RELATION_NAME = "3_grouptest1_table1";

	/**
	 * Test registering and unregistering the storage manager
	 * @throws StorageManagerException 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRegisterAndUnregister() throws StorageManagerException {
		Assert.assertFalse(StorageInterface.isStorageManagerActive(RELATION_NAME));
		StorageInterface.getStorageManager(RELATION_NAME);
		Assert.assertTrue(StorageInterface.isStorageManagerActive(RELATION_NAME));
		StorageInterface.shutdown(RELATION_NAME);
		Assert.assertFalse(StorageInterface.isStorageManagerActive(RELATION_NAME));
	}
}

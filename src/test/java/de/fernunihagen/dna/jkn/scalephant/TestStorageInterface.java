package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;

public class TestStorageInterface {
	
	/**
	 * The name of the test relation
	 */
	protected static final String RELATION_NAME = "test_storage";

	/**
	 * Test registering and unregistering the storage manager
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRegisterAndUnregister() {
		Assert.assertFalse(StorageInterface.isStorageManagerActive(RELATION_NAME));
		StorageInterface.getStorageManager(RELATION_NAME);
		Assert.assertTrue(StorageInterface.isStorageManagerActive(RELATION_NAME));
		StorageInterface.shutdown(RELATION_NAME);
		Assert.assertFalse(StorageInterface.isStorageManagerActive(RELATION_NAME));
	}
}

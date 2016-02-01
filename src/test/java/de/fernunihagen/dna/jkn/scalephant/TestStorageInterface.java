package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;

public class TestStorageInterface {
	
	/**
	 * Test registering and unregistering the storage manager
	 * @throws Exception
	 */
	@Test
	public void testRegisterAndUnregister() {
		Assert.assertFalse(StorageInterface.isStorageManagerActive("test_storage"));
		StorageInterface.getStorageManager("test_storage");
		Assert.assertTrue(StorageInterface.isStorageManagerActive("test_storage"));
		StorageInterface.shutdown("test_storage");
		Assert.assertFalse(StorageInterface.isStorageManagerActive("test_storage"));
	}
}

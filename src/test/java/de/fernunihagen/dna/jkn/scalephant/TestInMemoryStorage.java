package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.Storage;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManager;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class TestInMemoryStorage {
	
	protected static StorageManager storageManager;
	
	@BeforeClass
	public static void init() {
		storageManager = Storage.getStorageManager();
	}
	
	@AfterClass
	public static void shutdown() {
		storageManager.shutdown();
	}
	
	@Test
	public void testInsertElements() {
		final Tuple tuple = new Tuple();
		storageManager.put(1, tuple);
		Assert.assertEquals(tuple, storageManager.get(1));
	}
	
	@Test
	public void getNonExisting() {
		storageManager.clear();
		Assert.assertEquals(null, storageManager.get(1));
		Assert.assertEquals(null, storageManager.get(1000));
	}
}

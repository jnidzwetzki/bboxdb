package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManager;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;
import de.fernunihagen.dna.jkn.scalephant.util.ObjectSerializer;

public class TestInMemoryStorage {
	
	protected static StorageManager storageManager;
	protected final static String TEST_RELATION = "testrelation";
	
	@BeforeClass
	public static void init() {
		storageManager = StorageInterface.getStorageManager(TEST_RELATION);
	}
	
	@AfterClass
	public static void shutdown() {
		storageManager.shutdown();
	}
	
	@Test
	public void testInsertElements() throws Exception {
		final Tuple tuple = new Tuple(1, null, "abc".getBytes());

		storageManager.put(tuple);
		
		Assert.assertEquals(tuple, storageManager.get(1));
	}
	
	@Test
	public void testInsertAndReadPerson() throws Exception {
		final PersonEntity person1 = new PersonEntity("Jan", "Jansen", 30);
		final ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		final Tuple createdTuple = new Tuple(1, null, serializer.serialize(person1));
		
		storageManager.put(createdTuple);
		final Tuple readTuple = storageManager.get(1);
		
		final PersonEntity readPerson1 = serializer.deserialize(readTuple.getBytes());
		
		Assert.assertEquals(person1, readPerson1);
	}
	
	@Test
	public void getNonExisting() throws Exception {
		storageManager.clear();
		Assert.assertEquals(null, storageManager.get(1));
		Assert.assertEquals(null, storageManager.get(1000));
	}
}

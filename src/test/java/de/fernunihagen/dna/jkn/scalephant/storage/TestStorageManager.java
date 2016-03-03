package de.fernunihagen.dna.jkn.scalephant.storage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.PersonEntity;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.util.ObjectSerializer;

public class TestStorageManager {
	
	protected StorageManager storageManager;
	protected final static String TEST_RELATION = "1_testgroup1_abc";
	
	@Before
	public void init() throws StorageManagerException {
		storageManager = StorageInterface.getStorageManager(TEST_RELATION);
		storageManager.clear();
		Assert.assertTrue(storageManager.isReady());
	}
	
	@Test
	public void testInsertElements() throws Exception {
		final Tuple tuple = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes());
		storageManager.put(tuple);
		Assert.assertEquals(tuple, storageManager.get("1"));
	}
	
	@Test
	public void testInsertAndReadPerson() throws Exception {
		final PersonEntity person1 = new PersonEntity("Jan", "Jansen", 30);
		final ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		final Tuple createdTuple = new Tuple("1", BoundingBox.EMPTY_BOX, serializer.serialize(person1));
		
		storageManager.put(createdTuple);
		final Tuple readTuple = storageManager.get("1");
		
		final PersonEntity readPerson1 = serializer.deserialize(readTuple.getDataBytes());
		
		Assert.assertEquals(person1, readPerson1);
	}
	
	@Test
	public void getNonExisting() throws Exception {
		Assert.assertEquals(null, storageManager.get("1"));
		Assert.assertEquals(null, storageManager.get("1000"));
	}
	
	@Test
	public void testStoreNullTuple() throws Exception {
		final Tuple createdTuple = new Tuple("1", BoundingBox.EMPTY_BOX, null);
		storageManager.put(createdTuple);
		
		Assert.assertEquals(createdTuple, storageManager.get("1"));
	}
	
	@Test
	public void testTupleDelete() throws Exception {
		final Tuple createdTuple = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes());
		storageManager.put(createdTuple);
		
		Assert.assertEquals(createdTuple, storageManager.get("1"));
		
		storageManager.delete("1");
		Assert.assertEquals(null, storageManager.get("1"));
	}
	
	@Test
	public void testDeleteTuple() throws StorageManagerException, InterruptedException {
		int MAX_TUPLES = 100000;
		int SPECIAL_TUPLE = MAX_TUPLES / 2;
		
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
			
			if(i == SPECIAL_TUPLE) {
				storageManager.delete(Integer.toString(SPECIAL_TUPLE));
			}
		}
		
		// Let the storage manager swap the memtables out
		Thread.sleep(10000);
		
		final Tuple resultTuple = storageManager.get(Integer.toString(SPECIAL_TUPLE));
		
		Assert.assertEquals(null, resultTuple);
	}
	
	@Test
	public void testDeleteTuple2() throws StorageManagerException, InterruptedException {
		int MAX_TUPLES = 100000;
		int SPECIAL_TUPLE = MAX_TUPLES / 2;
		int DELETE_AFTER = (int) (MAX_TUPLES * 0.75);
		
		// Ensure that the tuple is not contained in the storage manager
		final Tuple resultTuple = storageManager.get(Integer.toString(SPECIAL_TUPLE));
		Assert.assertEquals(null, resultTuple);
		
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
			
			if(i == DELETE_AFTER) {
				storageManager.delete(Integer.toString(SPECIAL_TUPLE));
			}
		}
		
		// Let the storage manager swap the memtables out
		Thread.sleep(10000);
		
		// Fetch the deleted tuple
		final Tuple resultTuple2 = storageManager.get(Integer.toString(SPECIAL_TUPLE));
		Assert.assertEquals(null, resultTuple2);
	}
	
	@Test
	public void testBigInsert() throws Exception {
		int MAX_TUPLES = 100000;

		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple tuple = storageManager.get(Integer.toString(i));
			if(tuple == null) {
				System.out.println("Got null when requesting: " + i);
				Assert.assertNotNull(tuple);	
			}
			
			if(i % 1000 == 0) {
				System.out.println("Read tuples from storage manager: " + i);
			}
			
			Assert.assertEquals(Integer.toString(i), new String(tuple.getDataBytes()));
		}
	}
}

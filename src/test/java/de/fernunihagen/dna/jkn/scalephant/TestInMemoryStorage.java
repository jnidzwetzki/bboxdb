package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;
import de.fernunihagen.dna.jkn.scalephant.util.ObjectSerializer;

public class TestInMemoryStorage {
	
	protected static Memtable memtable;
	
	@BeforeClass
	public static void init() {
		memtable = new Memtable("test", 1000, 10000);
		memtable.init();
	}
	
	@AfterClass
	public static void shutdown() {
		memtable.shutdown();
	}
	
	@Before
	public void reinit() {
		memtable.clear();
	}
	
	@Test
	public void testInsertElements() throws Exception {
		final Tuple tuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(tuple);
		
		Assert.assertEquals(tuple, memtable.get("1"));
	}
	
	@Test
	public void testInsertAndReadPerson() throws Exception {
		final PersonEntity person1 = new PersonEntity("Jan", "Jansen", 30);
		final ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		final Tuple createdTuple = new Tuple("1", null, serializer.serialize(person1));
		
		memtable.put(createdTuple);
		final Tuple readTuple = memtable.get("1");
		
		final PersonEntity readPerson1 = serializer.deserialize(readTuple.getBytes());
		
		Assert.assertEquals(person1, readPerson1);
	}
	
	@Test
	public void getNonExisting() throws Exception {
		Assert.assertEquals(null, memtable.get("1"));
		Assert.assertEquals(null, memtable.get("1000"));
	}
	
	@Test
	public void testStoreNullTuple() throws Exception {		
		final Tuple createdTuple = new Tuple("1", null, null);
		memtable.put(createdTuple);
		
		Assert.assertEquals(createdTuple, memtable.get("1"));
	}
	
	@Test
	public void testTupleDelete() throws Exception {		
		final Tuple createdTuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple);
		
		Assert.assertEquals(createdTuple, memtable.get("1"));
		
		memtable.delete("1");
		Assert.assertEquals(null, memtable.get("1"));
	}
	
	@Test(expected=StorageManagerException.class)
	public void testBigInsert() throws Exception {		
		int MAX_TUPLES = 1000000;

		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), null, Integer.toString(i).getBytes());
			memtable.put(createdTuple);
		}
		
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple tuple = memtable.get(Integer.toString(i));
			Integer integer = Integer.parseInt(new String(tuple.getBytes()));
			Assert.assertEquals(Integer.toString(i), Integer.toString(integer));
		}
	}
	
	
	@Test
	public void testSortedList0() throws StorageManagerException {
		// Cleared memtables should return an empty list
		Assert.assertEquals(memtable.getSortedTupleList().size(), 0);
	}
	
	@Test
	public void testSortedList1() throws StorageManagerException {
		// Insert key 1
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);
		Assert.assertEquals(memtable.getSortedTupleList().size(), 1);
		
		// Update Key 1
		final Tuple createdTuple2 = new Tuple("1", null, "defh".getBytes());
		memtable.put(createdTuple2);
		Assert.assertEquals(memtable.getSortedTupleList().size(), 1);
	}
}

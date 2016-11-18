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


import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.PersonEntity;
import de.fernunihagen.dna.scalephant.storage.Memtable;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.util.ObjectSerializer;

public class TestInMemoryStorage {
	
	protected static Memtable memtable;
	
	@BeforeClass
	public static void init() {
		memtable = new Memtable(new SSTableName("3_mygroup_test"), 1000, 10000);
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
	
	/**
	 * Test insert1
	 * @throws Exception
	 */
	@Test
	public void testInsertElements() throws Exception {
		final Tuple tuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(tuple);
		
		Assert.assertEquals(tuple, memtable.get("1"));
	}
	
	/**
	 * Test insert2
	 * @throws Exception
	 */
	@Test
	public void testInsertAndReadPerson() throws Exception {
		final PersonEntity person1 = new PersonEntity("Jan", "Jansen", 30);
		final ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		final Tuple createdTuple = new Tuple("1", null, serializer.serialize(person1));
		
		memtable.put(createdTuple);
		final Tuple readTuple = memtable.get("1");
		
		final PersonEntity readPerson1 = serializer.deserialize(readTuple.getDataBytes());
		
		Assert.assertEquals(person1, readPerson1);
	}
	
	/**
	 * Query for non existing tuples
	 * @throws Exception
	 */
	@Test
	public void getNonExisting() throws Exception {
		Assert.assertEquals(null, memtable.get("1"));
		Assert.assertEquals(null, memtable.get("1000"));
	}
	
	/**
	 * Test the null tuple
	 * @throws Exception
	 */
	@Test
	public void testStoreNullTuple() throws Exception {		
		final Tuple createdTuple = new Tuple("1", null, null);
		memtable.put(createdTuple);
		
		Assert.assertEquals(createdTuple, memtable.get("1"));
	}
	
	/**
	 * Test the deletion of a tuple
	 * @throws Exception
	 */
	@Test
	public void testTupleDelete() throws Exception {		
		final Tuple createdTuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple);
		
		Assert.assertEquals(createdTuple, memtable.get("1"));
		memtable.delete("1");
		Assert.assertTrue(memtable.get("1") instanceof DeletedTuple);
	}
	
	/**
	 * Test memtable overflow
	 * @throws Exception
	 */
	@Test(expected=StorageManagerException.class)
	public void testBigInsert() throws Exception {	
		
		final int MAX_TUPLES = memtable.getMaxEntries() * 10;

		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), null, Integer.toString(i).getBytes());
			memtable.put(createdTuple);
		}
	}
	
	/**
	 * Test the sorted list query
	 * @throws StorageManagerException
	 */
	@Test
	public void testSortedList0() throws StorageManagerException {
		// Cleared memtables should return an empty list
		Assert.assertEquals(memtable.getSortedTupleList().size(), 0);
	}
	
	/**
	 * Test key updates and sorted list query
	 * @throws StorageManagerException
	 */
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
	
	/**
	 * The the time query
	 * @throws StorageManagerException
	 */
	@Test
	public void testTimeQuery() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "abc".getBytes());
		memtable.put(createdTuple2);
		
		final Tuple createdTuple3 = new Tuple("3", null, "abc".getBytes());
		memtable.put(createdTuple3);
		
		// Query the memtable
		Assert.assertEquals(3, memtable.getTuplesAfterTime(0).size());
		Assert.assertEquals(0, memtable.getTuplesAfterTime(Long.MAX_VALUE).size());
		
		Assert.assertEquals(2, memtable.getTuplesAfterTime(createdTuple1.getTimestamp()).size());
		Assert.assertEquals(1, memtable.getTuplesAfterTime(createdTuple2.getTimestamp()).size());
		Assert.assertEquals(0, memtable.getTuplesAfterTime(createdTuple3.getTimestamp()).size());
	}
	
	/**
	 * Test iterator 1
	 * @throws Exception
	 */
	@Test
	public void testIterate1() throws Exception {
		
		Assert.assertEquals(0, memtable.getSortedTupleList().size());
		
		int tuples = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : memtable) {
			tuples++;
		}
		
		Assert.assertEquals(0, tuples);
	}
	
	/**
	 * Test iterator 2
	 * @throws Exception
	 */
	@Test
	public void testIterate2() throws Exception {
		final Tuple tuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(tuple1);
		
		int tuples = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : memtable) {
			tuples++;
		}
		
		Assert.assertEquals(1, tuples);
	}
	
	/**
	 * Test iterator 3
	 * @throws Exception
	 */
	@Test
	public void testIterate3() throws Exception {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "abc".getBytes());
		memtable.put(createdTuple2);
		
		final Tuple createdTuple3 = new Tuple("3", null, "abc".getBytes());
		memtable.put(createdTuple3);
		
		int tuples = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : memtable) {
			tuples++;
		}
		
		Assert.assertEquals(3, tuples);
	}
	
}

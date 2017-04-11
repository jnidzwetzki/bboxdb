/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.storage;


import java.util.Iterator;
import java.util.List;

import org.bboxdb.PersonEntity;
import org.bboxdb.storage.Memtable;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.IteratorHelper;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateFilterIterator;
import org.bboxdb.util.MicroSecondTimestampProvider;
import org.bboxdb.util.ObjectSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMemtable {
	
	protected static Memtable memtable;
	
	@BeforeClass
	public static void init() {
		
		if(memtable != null) {
			memtable.shutdown();
		}
		
		memtable = new Memtable(new SSTableName("3_mygroup_test"), 1000, 10000);
		memtable.init();
		memtable.acquire();
	}
	
	@AfterClass
	public static void shutdown() {
		memtable.release();
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
		memtable.delete("1", MicroSecondTimestampProvider.getNewTimestamp());
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
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
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
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 1);
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "abc".getBytes(), 2);
		memtable.put(createdTuple2);
		
		final Tuple createdTuple3 = new Tuple("3", null, "abc".getBytes(), 3);
		memtable.put(createdTuple3);
		
		// Query the memtable
		Assert.assertEquals(3, countTuplesForPredicate(new NewerAsTimePredicate(0)));
		Assert.assertEquals(0, countTuplesForPredicate(new NewerAsTimePredicate(Long.MAX_VALUE)));
		
		Assert.assertEquals(2, countTuplesForPredicate(new NewerAsTimePredicate(createdTuple1.getVersionTimestamp())));
		Assert.assertEquals(1, countTuplesForPredicate(new NewerAsTimePredicate(createdTuple2.getVersionTimestamp())));
		Assert.assertEquals(0, countTuplesForPredicate(new NewerAsTimePredicate(createdTuple3.getVersionTimestamp())));
	}
	
	protected int countTuplesForPredicate(Predicate predicate) {
		final Iterator<Tuple> iterator = new PredicateFilterIterator(memtable.iterator(), predicate);
		return IteratorHelper.getIteratorSize(iterator);
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
	
	/**
	 * Test memtable empty
	 * @throws StorageManagerException 
	 */
	@Test
	public void testEmptyCall() throws StorageManagerException {
		init();
		Assert.assertTrue(memtable.isEmpty());
		memtable.clear();
		Assert.assertTrue(memtable.isEmpty());
		
		final Tuple createdTuple = new Tuple("3", null, "abc".getBytes());
		memtable.put(createdTuple);
		Assert.assertFalse(memtable.isEmpty());
		
		memtable.clear();
		Assert.assertTrue(memtable.isEmpty());
	}
	
	/**
	 * Test the tuple list generator
	 * @throws StorageManagerException
	 */
	@Test
	public void testGetTupleListCall() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "def".getBytes());
		memtable.put(createdTuple2);
		
		final List<Tuple> tupleList = memtable.getSortedTupleList();
		Assert.assertTrue(tupleList.contains(createdTuple1));
		Assert.assertTrue(tupleList.contains(createdTuple2));
		Assert.assertEquals(tupleList.size(), 2);
		
		memtable.clear();
		final List<Tuple> tupleList2 = memtable.getSortedTupleList();
		Assert.assertEquals(tupleList2.size(), 0);
	}
	
	/**
	 * Test newest and oldest timestamp
	 * @throws StorageManagerException
	 */
	public void testNewestOldest() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "def".getBytes(), 1);
		memtable.put(createdTuple2);
		
		Assert.assertEquals(1, memtable.getOldestTupleTimestampInMicroseconds());
		Assert.assertEquals(60, memtable.getNewestTupleTimestampMicroseconds());
		
		final DeletedTuple deletedTuple = new DeletedTuple("3", 500);
		memtable.put(deletedTuple);

		Assert.assertEquals(1, memtable.getOldestTupleTimestampInMicroseconds());
		Assert.assertEquals(500, memtable.getNewestTupleTimestampMicroseconds());
	}
	
}

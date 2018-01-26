/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.ObjectSerializer;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.memtable.Memtable;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsVersionTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateTupleFilterIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class TestMemtable {
	
	protected static Memtable memtable;
	
	@Before
	public void before() {
		memtable = new Memtable(new TupleStoreName("3_mygroup_test"), 1000, 10000);
		memtable.init();
		memtable.acquire();
	}
	
	@After
	public void after() {
		if(memtable != null) {
			memtable.release();
			memtable.shutdown();
			memtable = null;
		}
	}
	
	/**
	 * Test insert1
	 * @throws Exception
	 */
	@Test
	public void testInsertElements() throws Exception {
		final Tuple tuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(tuple);
		
		Assert.assertEquals(tuple, memtable.get("1").get(0));
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
		final List<Tuple> readTupleList = memtable.get("1");
		Assert.assertTrue(readTupleList.size() == 1);
		
		final Tuple readTuple = readTupleList.get(0);
		final PersonEntity readPerson1 = serializer.deserialize(readTuple.getDataBytes());
		
		Assert.assertEquals(person1, readPerson1);
	}
	
	/**
	 * Query for non existing tuples
	 * @throws Exception
	 */
	@Test
	public void getNonExisting() throws Exception {
		Assert.assertTrue(memtable.get("1").isEmpty());
		Assert.assertTrue(memtable.get("1000").isEmpty());
	}
	
	/**
	 * Test the null tuple
	 * @throws Exception
	 */
	@Test(expected=NullPointerException.class)
	public void testStoreNullTuple() throws Exception {		
		final Tuple createdTuple = new Tuple("1", null, null); // This should cause an NPE
		memtable.put(createdTuple);
		Assert.assertTrue(false);
	}
	
	/**
	 * Test the deletion of a tuple
	 * @throws Exception
	 */
	@Test
	public void testTupleDelete() throws Exception {		
		final Tuple createdTuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple);
		
		Assert.assertEquals(createdTuple, memtable.get("1").get(0));
		memtable.delete("1", MicroSecondTimestampProvider.getNewTimestamp());
		Assert.assertTrue(memtable.get("1").get(1) instanceof DeletedTuple);
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
		Assert.assertEquals(memtable.getSortedTupleList().size(), 2);
	}
	

	/**
	 * Test the sorted tuple list
	 * @throws StorageManagerException 
	 */
	@Test
	public void testSortedList3() throws StorageManagerException {
		// Insert key 1
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);
		Assert.assertEquals(memtable.getSortedTupleList().size(), 1);
		
		// Delete Key 1
		memtable.delete("1", System.currentTimeMillis());
		Assert.assertEquals(memtable.getSortedTupleList().size(), 2);
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
		Assert.assertEquals(3, countTuplesForPredicate(new NewerAsVersionTimePredicate(0)));
		Assert.assertEquals(0, countTuplesForPredicate(new NewerAsVersionTimePredicate(Long.MAX_VALUE)));
		
		Assert.assertEquals(2, countTuplesForPredicate(new NewerAsVersionTimePredicate(createdTuple1.getVersionTimestamp())));
		Assert.assertEquals(1, countTuplesForPredicate(new NewerAsVersionTimePredicate(createdTuple2.getVersionTimestamp())));
		Assert.assertEquals(0, countTuplesForPredicate(new NewerAsVersionTimePredicate(createdTuple3.getVersionTimestamp())));
	}
	
	/**
	 * Count the tuple for the given predicate
	 * @param predicate
	 * @return
	 */
	protected int countTuplesForPredicate(final Predicate predicate) {
		final Iterator<Tuple> iterator = new PredicateTupleFilterIterator(memtable.iterator(), predicate);
		return Iterators.size(iterator);
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
	@Test
	public void testNewestOldest() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "def".getBytes(), 1);
		memtable.put(createdTuple2);
		
		Assert.assertEquals(1, memtable.getOldestTupleVersionTimestamp());
		Assert.assertEquals(60, memtable.getNewestTupleVersionTimestamp());
		
		final DeletedTuple deletedTuple = new DeletedTuple("3", 500);
		memtable.put(deletedTuple);

		Assert.assertEquals(1, memtable.getOldestTupleVersionTimestamp());
		Assert.assertEquals(500, memtable.getNewestTupleVersionTimestamp());
	}
	
	/**
	 * Test the newest tuple insert timestamp
	 * @throws StorageManagerException 
	 */
	@Test
	public void newestTupleInsertTimestamp() throws StorageManagerException {
		final long timestamp1 = memtable.getNewestTupleInsertedTimestamp();
		Assert.assertTrue(timestamp1 > 0);
		
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);
		final long timestamp2 = memtable.getNewestTupleInsertedTimestamp();
		
		Assert.assertTrue(timestamp2 >= timestamp1);
	}
	
	/**
	 * Test the number of tuples
	 * @throws StorageManagerException 
	 */
	@Test
	public void testNumberOfTuples() throws StorageManagerException {
		Assert.assertEquals(0, memtable.getNumberOfTuples());
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);

		Assert.assertEquals(1, memtable.getNumberOfTuples());
	}
	
	/**
	 * Test the delete on close
	 * @throws StorageManagerException 
	 */
	@Test
	public void testDeleteOnShutdown() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);
		Assert.assertEquals(1, memtable.getNumberOfTuples());
		
		final boolean aquireResult1 = memtable.acquire();
		Assert.assertTrue(aquireResult1);

		Assert.assertFalse(memtable.isDeletePending());
		memtable.deleteOnClose();
		Assert.assertTrue(memtable.isDeletePending());
		Assert.assertFalse(memtable.isPersistent());
		
		// Release first aquire
		memtable.release();
		Assert.assertEquals(1, memtable.getNumberOfTuples());

		// Release second aquire
		memtable.release();
		Assert.assertEquals(0, memtable.getNumberOfTuples());

		// Aquire is not possible on deleted tables
		final boolean aquireResultAfterDelete = memtable.acquire();
		Assert.assertFalse(aquireResultAfterDelete);
		
		// Prevent release call in @after
		memtable = null;
	}
	
	/**
	 * Test get tuple at position
	 * @throws StorageManagerException 
	 */
	@Test
	public void getTupleAtPosition() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);
		
		final Tuple createdTuple2 = new Tuple("2", null, "def".getBytes(), 1);
		memtable.put(createdTuple2);
		
		final Tuple createdTuple3 = new Tuple("3", null, "def".getBytes(), 1);
		memtable.put(createdTuple3);
		
		Assert.assertEquals(createdTuple1, memtable.getTupleAtPosition(0));
		Assert.assertEquals(createdTuple2, memtable.getTupleAtPosition(1));
		Assert.assertEquals(createdTuple3, memtable.getTupleAtPosition(2));
	}
	
	/**
	 * Test the service name
	 */
	@Test
	public void testServicenameAndTablename() {
		Assert.assertTrue(memtable.getServicename() != null);
		Assert.assertTrue(memtable.getTupleStoreName() != null);
	}

}

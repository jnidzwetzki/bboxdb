/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.test.storage;


import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.ObjectSerializer;
import org.bboxdb.commons.entity.PersonEntity;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.query.queryprocessor.predicate.NewerAsVersionTimePredicate;
import org.bboxdb.query.queryprocessor.predicate.Predicate;
import org.bboxdb.query.queryprocessor.predicate.PredicateTupleFilterIterator;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.memtable.Memtable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class TestMemtable {

	/**
	 * The table name
	 */
	private static final TupleStoreName MEMTABLE_TABLE_NAME = new TupleStoreName("3_mygroup_test");

	/**
	 * The max amount of entries
	 */
	private static final int MEMTABLE_MAX_ENTRIES = 1000;

	/**
	 * The max size of a memtable
	 */
	private static final int MEMTABLE_MAX_SIZE = 10000;

	/**
	 * The memtable reference
	 */
	private static Memtable memtable;

	@Before
	public void before() {
		memtable = new Memtable(MEMTABLE_TABLE_NAME, MEMTABLE_MAX_ENTRIES, MEMTABLE_MAX_SIZE, null);
		memtable.init();
		memtable.acquire();
	}

	@After
	public void after() {
		if(memtable != null) {
			memtable.deleteOnClose();
			memtable.release();
			memtable.shutdown();
			memtable = null;
		}
	}

	/**
	 * Test insert1
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void testInsertElements() throws Exception {
		final Tuple tuple = new Tuple("1", null, "abc".getBytes());
		memtable.put(tuple);

		Assert.assertEquals(tuple, memtable.get("1").get(0));
	}

	/**
	 * Test insert2
	 * @throws Exception
	 */
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
			final Tuple createdTuple = new Tuple(Integer.toString(i), Hyperrectangle.FULL_SPACE, Integer.toString(i).getBytes());
			memtable.put(createdTuple);
		}
	}

	/**
	 * Test the sorted list query
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
	public void testSortedList0() throws StorageManagerException {
		// Cleared memtables should return an empty list
		Assert.assertEquals(memtable.getSortedTupleList().size(), 0);
	}

	/**
	 * Test key updates and sorted list query
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	 * Test iterator 4
	 * @throws Exception
	 */
	@Test(expected=IllegalStateException.class)
	public void testIterate4() throws Exception {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);

		final Tuple createdTuple2 = new Tuple("2", null, "abc".getBytes());
		memtable.put(createdTuple2);

		final Tuple createdTuple3 = new Tuple("3", null, "abc".getBytes());
		memtable.put(createdTuple3);

		final Iterator<Tuple> iter = memtable.iterator();
		iter.remove();
	}

	/**
	 * Test iterator 5
	 * @throws Exception
	 */
	@Test(expected=IllegalStateException.class, timeout=10000)
	public void testIterate5() throws Exception {
		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes());
		memtable.put(createdTuple1);

		final Tuple createdTuple2 = new Tuple("2", null, "abc".getBytes());
		memtable.put(createdTuple2);

		final Tuple createdTuple3 = new Tuple("3", null, "abc".getBytes());
		memtable.put(createdTuple3);

		final Iterator<Tuple> iter = memtable.iterator();

		while(true) {
			iter.next();
		}
	}

	/**
	 * Test memtable empty
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	 * Test bounding box - full space
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
	public void testBoundingBoxFullSpace() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 60);
		memtable.put(createdTuple1);

		final Tuple createdTuple2 = new Tuple("2", Hyperrectangle.FULL_SPACE, "def".getBytes(), 1);
		memtable.put(createdTuple2);

		final Iterator<Tuple> tuples = memtable.getAllTuplesInBoundingBox(Hyperrectangle.FULL_SPACE);
		final List<Tuple> resultList = Lists.newArrayList(tuples);

		Assert.assertEquals(2, resultList.size());
	}

	/**
	 * Test bounding box - part space
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
	public void testBoundingBoxPartSpace() throws StorageManagerException {
		final Tuple createdTuple1 = new Tuple("1", new Hyperrectangle(1.0, 2.0, 1.0, 2.0), "abc".getBytes(), 60);
		memtable.put(createdTuple1);

		final Tuple createdTuple2 = new Tuple("2", new Hyperrectangle(10.0, 15.0, 10.0, 15.0), "def".getBytes(), 1);
		memtable.put(createdTuple2);

		final Iterator<Tuple> tuples1 = memtable.getAllTuplesInBoundingBox(Hyperrectangle.FULL_SPACE);
		final List<Tuple> resultList1 = Lists.newArrayList(tuples1);
		Assert.assertEquals(2, resultList1.size());

		final Iterator<Tuple> tuples2 = memtable.getAllTuplesInBoundingBox(new Hyperrectangle(1.2, 1.5, 1.2, 1.5));
		final List<Tuple> resultList2 = Lists.newArrayList(tuples2);
		Assert.assertEquals(1, resultList2.size());

		final Iterator<Tuple> tuples3 = memtable.getAllTuplesInBoundingBox(new Hyperrectangle(1.0, 30.0, 1.0, 30.0));
		final List<Tuple> resultList3 = Lists.newArrayList(tuples3);
		Assert.assertEquals(2, resultList3.size());
	}

	/**
	 * Test the newest tuple insert timestamp
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
	public void testServicenameAndTablename() {
		Assert.assertTrue(memtable.getServicename() != null);
		Assert.assertTrue(memtable.getTupleStoreName() != null);
	}

	/**
	 * Test if full
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
	public void isFullBySize() throws StorageManagerException {
		Assert.assertFalse(memtable.isFull());

		final Tuple tuple = new Tuple("3", null, "def".getBytes(), 1) {
			@Override
			public int getSize() {
				return (int) ((MEMTABLE_MAX_SIZE / 2) - 1);
			}
		};

		memtable.put(tuple);
		Assert.assertFalse(memtable.isFull());
		memtable.put(tuple);
		Assert.assertFalse(memtable.isFull());
		memtable.put(tuple);
		Assert.assertTrue(memtable.isFull());
	}

	/**
	 * Test the reinit
	 */
	@Test(timeout=60000)
	public void testReinit() {
		memtable.init();
	}

	/**
	 * Test the to string method
	 */
	@Test(timeout=60000)
	public void testToString() {
		Assert.assertTrue(memtable.toString().length() > 10);
	}

	/**
	 * Test the aquire
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
	public void testAquire1() throws StorageManagerException {
		final Memtable memtable = new Memtable(MEMTABLE_TABLE_NAME, MEMTABLE_MAX_ENTRIES,
				MEMTABLE_MAX_SIZE, null);
		memtable.init();

		Assert.assertTrue(memtable.acquire());

		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);

		memtable.deleteOnClose();
		Assert.assertFalse(memtable.acquire());

		Assert.assertEquals(1, memtable.get("1").size());
		memtable.release();
		Assert.assertEquals(0, memtable.getSize());
	}

	/**
	 * Test the aquire
	 * @throws StorageManagerException
	 */
	@Test(timeout=60000)
	public void testAquire2() throws StorageManagerException {
		final Memtable memtable = new Memtable(MEMTABLE_TABLE_NAME, MEMTABLE_MAX_ENTRIES,
				MEMTABLE_MAX_SIZE, null);
		memtable.init();

		Assert.assertTrue(memtable.acquire());

		final Tuple createdTuple1 = new Tuple("1", null, "abc".getBytes(), 60);
		memtable.put(createdTuple1);
		memtable.release();
		Assert.assertEquals(3, memtable.getSize());
		memtable.deleteOnClose();
		Assert.assertEquals(0, memtable.getSize());
	}
}

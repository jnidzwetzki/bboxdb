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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bboxdb.PersonEntity;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.ObjectSerializer;
import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStorageManager {
	
	/**
	 * The instance of the storage manager
	 */
	protected TupleStoreManager storageManager;
	
	/**
	 * The name of the test relation
	 */
	protected final static TupleStoreName TEST_RELATION = new TupleStoreName("testgroup1_abc");
	
	/**
	 * The amount of tuples for the big insert test
	 */
	protected int BIG_INSERT_TUPLES = 1000000;
	
	/**
	 * The storage registry
	 */
	protected static TupleStoreManagerRegistry storageRegistry;
	
	@BeforeClass
	public static void beforeClass() throws InterruptedException, BBoxDBException {
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
	}
	
	@AfterClass
	public static void afterClass() {
		if(storageRegistry != null) {
			storageRegistry.shutdown();
			storageRegistry = null;
		}
	}

	@Before
	public void init() throws StorageManagerException {
		// Delete the old table
		storageRegistry.deleteTable(TEST_RELATION);
		
		// Create a new table
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder.create().build();
		storageRegistry.createTable(TEST_RELATION, tupleStoreConfiguration);
		
		// Assure table is created successfully
		storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);
		Assert.assertTrue(storageManager.getServiceState().isInRunningState());
	}
	
	@Test(timeout=10000)
	public void testFlushEmptyTable() {
		storageManager.flush();
	}
	
	@Test
	public void testInsertElements1() throws Exception {
		final Tuple tuple = new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes());
		storageManager.put(tuple);
		Assert.assertEquals(tuple, storageManager.get("1").get(0));
	}
	
	@Test
	public void testInsertElements2() throws Exception {
		final Tuple tuple1 = new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes());
		final Tuple tuple2 = new Tuple("1", BoundingBox.FULL_SPACE, "def".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		
		Assert.assertEquals(tuple2, storageManager.get("1").get(0));
	}
	
	@Test
	public void testInsertAndReadPerson() throws Exception {
		final PersonEntity person1 = new PersonEntity("Jan", "Jansen", 30);
		final ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		final Tuple createdTuple = new Tuple("1", BoundingBox.FULL_SPACE, serializer.serialize(person1));
		
		storageManager.put(createdTuple);
		final List<Tuple> readTuples = storageManager.get("1");
		
		Assert.assertTrue(readTuples.size() == 1);
		
		final Tuple readTuple = readTuples.get(0);
		
		final PersonEntity readPerson1 = serializer.deserialize(readTuple.getDataBytes());
		
		Assert.assertEquals(person1, readPerson1);
	}
	
	@Test
	public void getNonExisting() throws Exception {
		Assert.assertTrue(storageManager.get("1").isEmpty());
		Assert.assertTrue(storageManager.get("1000").isEmpty());
	}
	
	@Test(expected=NullPointerException.class)
	public void testStoreNullTuple() throws Exception {
		final Tuple createdTuple = new Tuple("1", BoundingBox.FULL_SPACE, null); // This should cause an NPE
		storageManager.put(createdTuple);
		Assert.assertTrue(false);
	}
	
	@Test
	public void testInsertCallbacks() throws StorageManagerException, RejectedException {
		final List<Tuple> receivedTuples = new ArrayList<>();
		final Consumer<Tuple> callback = ((t) -> receivedTuples.add(t));
		
		storageManager.registerInsertCallback(callback);
		
		final Tuple createdTuple1 = new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes());
		final Tuple createdTuple2 = new Tuple("2", BoundingBox.FULL_SPACE, "abc".getBytes());
		final Tuple createdTuple3 = new Tuple("3", BoundingBox.FULL_SPACE, "abc".getBytes());

		storageManager.put(createdTuple1);
		Assert.assertEquals(1, receivedTuples.size());
		
		storageManager.put(createdTuple2);
		Assert.assertEquals(2, receivedTuples.size());
		
		final boolean removeResult1 = storageManager.removeInsertCallback(callback);
		Assert.assertTrue(removeResult1);
		final boolean removeResult2 = storageManager.removeInsertCallback(callback);
		Assert.assertFalse(removeResult2);
		
		storageManager.put(createdTuple3);
		Assert.assertEquals(2, receivedTuples.size());
	}
	
	@Test
	public void testTupleDelete() throws Exception {
		final Tuple createdTuple = new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes());
		storageManager.put(createdTuple);
		
		Assert.assertEquals(createdTuple, storageManager.get("1").get(0));
		
		storageManager.delete("1", MicroSecondTimestampProvider.getNewTimestamp());
		Assert.assertTrue(storageManager.get("1").get(0) instanceof DeletedTuple);
	}
	
	@Test
	public void testDeleteTuple() throws StorageManagerException, InterruptedException, RejectedException {
		int MAX_TUPLES = 100000;
		int SPECIAL_TUPLE = MAX_TUPLES / 2;
		
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
			
			if(i == SPECIAL_TUPLE) {
				storageManager.delete(Integer.toString(SPECIAL_TUPLE), createdTuple.getVersionTimestamp() + 1);
			}
		}
		
		storageManager.flush();
		
		final List<Tuple> readTuples = storageManager.get(Integer.toString(SPECIAL_TUPLE));
		
		Assert.assertEquals(1, readTuples.size());
		Assert.assertTrue(readTuples.get(0) instanceof DeletedTuple);
	}
	
	@Test
	public void testDeleteTuple2() throws StorageManagerException, InterruptedException, RejectedException {
		int MAX_TUPLES = 100000;
		int SPECIAL_TUPLE = MAX_TUPLES / 2;
		int DELETE_AFTER = (int) (MAX_TUPLES * 0.75);
		
		// Ensure that the tuple is not contained in the storage manager
		final List<Tuple> readTuples = storageManager.get(Integer.toString(SPECIAL_TUPLE));
		Assert.assertTrue(readTuples.isEmpty());
		
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
			
			if(i == DELETE_AFTER) {
				storageManager.delete(Integer.toString(SPECIAL_TUPLE), MicroSecondTimestampProvider.getNewTimestamp());
			}
		}
		
		// Let the storage manager swap the memtables out
		storageManager.flush();	
		
		// Fetch the deleted tuple
		final List<Tuple> readTuples2 = storageManager.get(Integer.toString(SPECIAL_TUPLE));
				
		Assert.assertEquals(1, readTuples2.size());
		Assert.assertTrue(readTuples2.get(0) instanceof DeletedTuple);
	}
	
	/**
	 * Test the mass deletion of tuples
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws RejectedException 
	 */
	@Test
	public void testDeleteTuple3() throws StorageManagerException, InterruptedException, RejectedException {
		int MAX_TUPLES = getNumberOfTuplesForBigInsert();
		
		System.out.println("Inserting tuples...");
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		Thread.sleep(1000);
		
		System.out.println("Deleting tuples...");
		for(int i = 0; i < MAX_TUPLES; i++) {
			storageManager.delete(Integer.toString(i), MicroSecondTimestampProvider.getNewTimestamp());
		}
		
		storageManager.flush();
		
		System.out.println("Reading tuples...");
		// Fetch the deleted tuples
		for(int i = 0; i < MAX_TUPLES; i++) {
			final List<Tuple> readTuples = storageManager.get(Integer.toString(i));
			
			Assert.assertEquals(1, readTuples.size());
			Assert.assertTrue(readTuples.get(0) instanceof DeletedTuple);
		}
	}
	/*
	@Test
	public void testBigInsert() throws Exception {

		final int BIG_INSERT_TUPLES = getNumberOfTuplesForBigInsert();

		for(int i = 0; i < BIG_INSERT_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		for(int i = 0; i < BIG_INSERT_TUPLES; i++) {
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
	}*/
	
	/**
	 * Number of tuples for big insert
	 * @return
	 */
	protected int getNumberOfTuplesForBigInsert() {
		return 1000000;
	}
	

	/**
	 * Test the storage manager with duplicates
	 * @throws StorageManagerException 
	 * @throws RejectedException 
	 */
	@Test
	public void testWithDuplicates() throws StorageManagerException, RejectedException {
		
		// Delete the old table
		storageRegistry.deleteTable(TEST_RELATION);
		
		// Create a new table
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder
				.create()
				.allowDuplicates(true)
				.build();
		
		storageRegistry.createTable(TEST_RELATION, tupleStoreConfiguration);
		
		// Assure table is created successfully
		storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);
		Assert.assertTrue(storageManager.getServiceState().isInRunningState());
		
		final Tuple tuple1 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc1".getBytes());
		storageManager.put(tuple1);
		
		final Tuple tuple2 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc2".getBytes());
		storageManager.put(tuple2);

		final Tuple tuple3 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc3".getBytes());
		storageManager.put(tuple3);

		final Tuple tuple4 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc4".getBytes());
		storageManager.put(tuple4);

		final Tuple tuple5 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc5".getBytes());
		storageManager.put(tuple5);

		final List<Tuple> readTuples = storageManager.get("abc");
		Assert.assertEquals(5, readTuples.size());
		Assert.assertTrue(readTuples.contains(tuple1));
		Assert.assertTrue(readTuples.contains(tuple2));
		Assert.assertTrue(readTuples.contains(tuple3));
		Assert.assertTrue(readTuples.contains(tuple4));
		Assert.assertTrue(readTuples.contains(tuple5));
	}
	
	/**
	 * Test the storage manager with duplicates
	 * @throws StorageManagerException 
	 * @throws RejectedException 
	 */
	@Test
	public void testVersionDuplicates() throws StorageManagerException, RejectedException {
		
		// Delete the old table
		storageRegistry.deleteTable(TEST_RELATION);
		
		// Create a new table
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder
				.create()
				.allowDuplicates(true)
				.withVersions(3)
				.build();
		
		storageRegistry.createTable(TEST_RELATION, tupleStoreConfiguration);
		
		// Assure table is created successfully
		storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);
		Assert.assertTrue(storageManager.getServiceState().isInRunningState());
		
		final Tuple tuple1 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc1".getBytes());
		storageManager.put(tuple1);
		
		final Tuple tuple2 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc2".getBytes());
		storageManager.put(tuple2);

		final Tuple tuple3 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc3".getBytes());
		storageManager.put(tuple3);

		final Tuple tuple4 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc4".getBytes());
		storageManager.put(tuple4);

		final Tuple tuple5 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc5".getBytes());
		storageManager.put(tuple5);

		final List<Tuple> readTuples = storageManager.get("abc");
		Assert.assertEquals(3, readTuples.size());
		Assert.assertTrue(readTuples.contains(tuple3));
		Assert.assertTrue(readTuples.contains(tuple4));
		Assert.assertTrue(readTuples.contains(tuple5));
	}
	
	/**
	 * Test the storage manager with duplicates - ttl version
	 * @throws StorageManagerException 
	 * @throws RejectedException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testTTLDuplicates() throws StorageManagerException, RejectedException, InterruptedException {
		
		// The TTL
		final int TTL_IN_MS = 5000;
		
		// Delete the old table
		storageRegistry.deleteTable(TEST_RELATION);
		
		// Create a new table
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder
				.create()
				.allowDuplicates(true)
				.withTTL(TTL_IN_MS, TimeUnit.MILLISECONDS)
				.build();
		
		storageRegistry.createTable(TEST_RELATION, tupleStoreConfiguration);
		
		// Assure table is created successfully
		storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);
		Assert.assertTrue(storageManager.getServiceState().isInRunningState());
		
		final Tuple tuple1 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc1".getBytes());
		storageManager.put(tuple1);
		
		final Tuple tuple2 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc2".getBytes());
		storageManager.put(tuple2);

		final Tuple tuple3 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc3".getBytes());
		storageManager.put(tuple3);

		final Tuple tuple4 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc4".getBytes());
		storageManager.put(tuple4);

		final Tuple tuple5 = new Tuple("abc", BoundingBox.FULL_SPACE, "abc5".getBytes());
		storageManager.put(tuple5);

		final List<Tuple> readTuples = storageManager.get("abc");
		Assert.assertFalse(readTuples.isEmpty());
		
		// Sleep longer than TTL
		Thread.sleep(TTL_IN_MS * 2);
		
		final List<Tuple> readTuples2 = storageManager.get("abc");
		Assert.assertTrue(readTuples2.isEmpty());
	}
}

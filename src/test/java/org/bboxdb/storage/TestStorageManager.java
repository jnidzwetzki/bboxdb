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

import org.bboxdb.PersonEntity;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.util.MicroSecondTimestampProvider;
import org.bboxdb.util.ObjectSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStorageManager {
	
	/**
	 * The instance of the storage manager
	 */
	protected SSTableManager storageManager;
	
	/**
	 * The name of the test relation
	 */
	protected final static SSTableName TEST_RELATION = new SSTableName("1_testgroup1_abc");
	
	/**
	 * The amount of tuples for the big insert test
	 */
	protected int BIG_INSERT_TUPLES = 1000000;

	
	@Before
	public void init() throws StorageManagerException {
		storageManager = StorageRegistry.getInstance().getSSTableManager(TEST_RELATION);
		storageManager.clear();
		Assert.assertTrue(storageManager.isReady());
	}
	
	@Test
	public void testInsertElements1() throws Exception {
		final Tuple tuple = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes());
		storageManager.put(tuple);
		Assert.assertEquals(tuple, storageManager.get("1"));
	}
	
	@Test
	public void testInsertElements2() throws Exception {
		final Tuple tuple1 = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes());
		final Tuple tuple2 = new Tuple("1", BoundingBox.EMPTY_BOX, "def".getBytes());

		storageManager.put(tuple1);
		storageManager.put(tuple2);
		
		Assert.assertEquals(tuple2, storageManager.get("1"));
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
		
		storageManager.delete("1", MicroSecondTimestampProvider.getNewTimestamp());
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
				storageManager.delete(Integer.toString(SPECIAL_TUPLE), createdTuple.getTimestamp() + 1);
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
				storageManager.delete(Integer.toString(SPECIAL_TUPLE), MicroSecondTimestampProvider.getNewTimestamp());
			}
		}
		
		// Let the storage manager swap the memtables out
		Thread.sleep(10000);
		
		// Fetch the deleted tuple
		final Tuple resultTuple2 = storageManager.get(Integer.toString(SPECIAL_TUPLE));
		Assert.assertEquals(null, resultTuple2);
	}
	
	/**
	 * Test the mass deletion of tuples
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 */
	@Test
	public void testDeleteTuple3() throws StorageManagerException, InterruptedException {
		int MAX_TUPLES = getNumberOfTuplesForBigInsert();
		
		System.out.println("Inserting tuples...");
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple createdTuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, Integer.toString(i).getBytes());
			storageManager.put(createdTuple);
		}
		
		Thread.sleep(1000);
		
		System.out.println("Deleting tuples...");
		for(int i = 0; i < MAX_TUPLES; i++) {
			storageManager.delete(Integer.toString(i), MicroSecondTimestampProvider.getNewTimestamp());
		}
		
		// Let the storage manager swap the memtables out
		Thread.sleep(10000);
		
		System.out.println("Reading tuples...");
		// Fetch the deleted tuples
		for(int i = 0; i < MAX_TUPLES; i++) {
			final Tuple resultTuple2 = storageManager.get(Integer.toString(i));
			Assert.assertEquals(null, resultTuple2);
		}
	}
	
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
	}
	
	/**
	 * Number of tuples for big insert
	 * @return
	 */
	protected int getNumberOfTuplesForBigInsert() {
		return 1000000;
	}
}

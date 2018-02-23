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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.compact.SSTableCompactor;
import org.bboxdb.storage.sstable.reader.SSTableKeyIndexReader;
import org.bboxdb.storage.sstable.reader.SSTableReader;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestTableCompactor {
	
	/**
	 * The output relation name
	 */
	protected final static TupleStoreName TEST_RELATION = new TupleStoreName("testgroup1_relation1");
	
	/**
	 * The storage directory
	 */
	protected static final String STORAGE_DIRECTORY = BBoxDBConfigurationManager.getConfiguration().getStorageDirectories().get(0);

	/**
	 * The max number of expected tuples in the sstable
	 */
	protected final static int EXPECTED_TUPLES = 100;
	
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
	public void clearData() throws StorageManagerException {
		storageRegistry.deleteTable(TEST_RELATION);
		final String relationDirectory = SSTableHelper.getSSTableDir(STORAGE_DIRECTORY, TEST_RELATION);
		final File relationDirectoryFile = new File(relationDirectory);
		relationDirectoryFile.mkdirs();
	}

	@Test
	public void testCompactTestFileCreation() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.FULL_SPACE, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
				
		storageRegistry.deleteTable(TEST_RELATION);
		storageRegistry.createTable(TEST_RELATION, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);
		
		final SSTableCompactor compactor = new SSTableCompactor(storageManager, Arrays.asList(reader1, reader2));
		compactor.executeCompactation();
		final List<SSTableWriter> resultWriter = compactor.getResultList();
		
		Assert.assertEquals(1, resultWriter.size());
		Assert.assertEquals(2, compactor.getReadTuples());
		Assert.assertEquals(2, compactor.getWrittenTuples());
		
		for(final SSTableWriter writer : resultWriter) {
			Assert.assertTrue(writer.getSstableFile().exists());
			Assert.assertTrue(writer.getSstableIndexFile().exists());
			writer.close();
		}
		
	}
	
	@Test
	public void testCompactTestMerge() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.FULL_SPACE, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		int counter = 0;
		
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	@Test
	public void testCompactTestMergeBig() throws StorageManagerException {
		
		SSTableKeyIndexReader reader1 = null;
		SSTableKeyIndexReader reader2 = null;
		final List<Tuple> tupleList = new ArrayList<Tuple>();

		for(int i = 0; i < 500; i=i+2) {
			tupleList.add(new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, "abc".getBytes()));
		}
		reader1 = addTuplesToFileAndGetReader(tupleList, 5);

		tupleList.clear();
	
		for(int i = 1; i < 500; i=i+2) {
			tupleList.add(new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, "def".getBytes()));
		}
		reader2 = addTuplesToFileAndGetReader(tupleList, 2);

		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		
		// Check the amount of tuples
		int counter = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}
		Assert.assertEquals(500, counter);
		
		// Check the consistency of the index
		for(int i = 1; i < 500; i++) {
			final List<Integer> positions = ssTableIndexReader.getPositionsForTuple(Integer.toString(i));
			Assert.assertTrue(positions.size() == 1);
		}
		
	}
	
	
	@Test
	public void testCompactTestFileOneEmptyfile1() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		int counter = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	@Test
	public void testCompactTestFileOneEmptyfile2() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.FULL_SPACE, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
				
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		int counter = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	@Test
	public void testCompactTestSameKey() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes(), 1));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("1", BoundingBox.FULL_SPACE, "def".getBytes(), 2));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
				
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			Assert.assertTrue(tuple.getKey().equals("1"));
			counter++;
		}		
				
		Assert.assertEquals(1, counter);
	}	
	
	/**
	 * Run the compactification with one deleted tuple
	 * @throws StorageManagerException
	 */
	@Test
	public void testCompactTestWithDeletedTuple() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new DeletedTuple("2"));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
				
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		
		int counter = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}		
				
		Assert.assertEquals(2, counter);
	}	
	
	/**
	 * Test a minor compactation
	 * @throws StorageManagerException
	 */
	@Test
	public void testCompactationMinor() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new DeletedTuple("2"));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
				
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, false);
		
		boolean containsDeletedTuple = false;
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			if(tuple instanceof DeletedTuple) {
				containsDeletedTuple = true;
			}
		}		
				
		Assert.assertEquals(2, counter);
		Assert.assertTrue(containsDeletedTuple);
	}
	
	/**
	 * Test a major compactation
	 * @throws StorageManagerException
	 */
	@Test
	public void testCompactationMajor1() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new DeletedTuple("2"));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, true);
		
		boolean containsDeletedTuple = false;
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			if(tuple instanceof DeletedTuple) {
				containsDeletedTuple = true;
			}
		}		
				
		Assert.assertEquals(1, counter);
		Assert.assertFalse(containsDeletedTuple);
	}
	
	/**
	 * Test a major compactation
	 * @throws StorageManagerException
	 */
	@Test
	public void testCompactationMajor2() throws StorageManagerException {
		
		final List<Tuple> tupleList1 = new ArrayList<>();

		final Tuple nonDeletedTuple = new Tuple("KEY", BoundingBox.FULL_SPACE, "abc".getBytes());
		tupleList1.add(nonDeletedTuple);

		for(int i = 0; i < 1000; i++) {
			tupleList1.add(new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, "abc".getBytes()));
		}
		
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		for(int i = 0; i < 1000; i++) {
			tupleList2.add(new DeletedTuple(Integer.toString(i)));
		}
		
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, true);
		
		final Iterator<Tuple> iterator = ssTableIndexReader.iterator();
		List<Tuple> tupes = Lists.newArrayList(iterator);
		
		Assert.assertEquals(1, tupes.size());
		Assert.assertTrue(tupes.contains(nonDeletedTuple));
	}

	/**
	 * Execute a compactification and return the reader for the resulting table
	 * 
	 * @param reader1
	 * @param reader2
	 * @param writer
	 * @param major 
	 * @return
	 * @throws StorageManagerException
	 */
	protected SSTableKeyIndexReader exectuteCompactAndGetReader(
			final SSTableKeyIndexReader reader1,
			final SSTableKeyIndexReader reader2, final boolean majorCompaction)
			throws StorageManagerException {
		
		storageRegistry.deleteTable(TEST_RELATION);
		storageRegistry.createTable(TEST_RELATION, new TupleStoreConfiguration());
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);
		
		final SSTableCompactor compactor = new SSTableCompactor(storageManager, Arrays.asList(reader1, reader2));
		compactor.setMajorCompaction(majorCompaction);
		compactor.executeCompactation();
		final List<SSTableWriter> resultWriter = compactor.getResultList();
		
		Assert.assertEquals(1, resultWriter.size());
		
		final SSTableWriter writer = resultWriter.get(0);
		final SSTableReader reader = new SSTableReader(STORAGE_DIRECTORY, TEST_RELATION, writer.getTablenumber());
		reader.init();
		
		final SSTableKeyIndexReader ssTableIndexReader = new SSTableKeyIndexReader(reader);
		ssTableIndexReader.init();
		
		return ssTableIndexReader;
	}
	
	/**
	 * Write the tuplelist into a SSTable and return a reader for this table
	 * 
	 * @param tupleList
	 * @param number
	 * @return
	 * @throws StorageManagerException
	 */
	protected SSTableKeyIndexReader addTuplesToFileAndGetReader(final List<Tuple> tupleList, int number)
			throws StorageManagerException {

		Collections.sort(tupleList);
		
		final SSTableWriter ssTableWriter = new SSTableWriter(STORAGE_DIRECTORY, TEST_RELATION, number, EXPECTED_TUPLES);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		ssTableWriter.close();
		
		final SSTableReader sstableReader = new SSTableReader(STORAGE_DIRECTORY, TEST_RELATION, number);
		sstableReader.init();
		final SSTableKeyIndexReader ssTableIndexReader = new SSTableKeyIndexReader(sstableReader);
		ssTableIndexReader.init();
		
		return ssTableIndexReader;
	}
	
}

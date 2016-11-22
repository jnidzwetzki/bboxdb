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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableWriter;
import de.fernunihagen.dna.scalephant.storage.sstable.compact.SSTableCompactor;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableKeyIndexReader;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableReader;

public class TestTableCompactor {
	
	protected final static SSTableName TEST_RELATION = new SSTableName("1_testgroup1_relation1");
	
	protected static final String DATA_DIRECTORY = ScalephantConfigurationManager.getConfiguration().getDataDirectory();

	@Before
	public void clearData() throws StorageManagerException {
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	}

	@Test
	public void testCompactTestFileCreation() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		boolean compactResult = compactor.executeCompactation();
		
		Assert.assertTrue(compactResult);
		Assert.assertTrue(writer.getSstableFile().exists());
		Assert.assertTrue(writer.getSstableIndexFile().exists());
		
		Assert.assertEquals(2, compactor.getReadTuples());
		Assert.assertEquals(2, compactor.getWrittenTuples());
		
		writer.close();
	}
	
	@Test
	public void testCompactTestMerge() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
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
			tupleList.add(new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, "abc".getBytes()));
		}
		reader1 = addTuplesToFileAndGetReader(tupleList, 5);

		tupleList.clear();
	
		for(int i = 1; i < 500; i=i+2) {
			tupleList.add(new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, "def".getBytes()));
		}
		reader2 = addTuplesToFileAndGetReader(tupleList, 2);

		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
		
		// Check the amount of tuples
		int counter = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}
		Assert.assertEquals(500, counter);
		
		// Check the consistency of the index
		for(int i = 1; i < 500; i++) {
			int pos = ssTableIndexReader.getPositionForTuple(Integer.toString(i));
			Assert.assertTrue(pos != -1);
		}
		
	}
	
	
	@Test
	public void testCompactTestFileOneEmptyfile1() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
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
		tupleList2.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
		int counter = 0;
		for(@SuppressWarnings("unused") final Tuple tuple : ssTableIndexReader) {
			counter++;
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	@Test
	public void testCompactTestSameKey() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("1", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
		
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			Assert.assertEquals("def", new String(tuple.getDataBytes()));
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
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new DeletedTuple("2"));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
		
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
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new DeletedTuple("2"));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, false);
		
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
	 * Test a minor compactation
	 * @throws StorageManagerException
	 */
	@Test
	public void testCompactationMajor() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableKeyIndexReader reader1 = addTuplesToFileAndGetReader(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new DeletedTuple("2"));
		final SSTableKeyIndexReader reader2 = addTuplesToFileAndGetReader(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 3);
		
		final SSTableKeyIndexReader ssTableIndexReader = exectuteCompactAndGetReader(
				reader1, reader2, writer, true);
		
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
			final SSTableKeyIndexReader reader2, final SSTableWriter writer, final boolean majorCompaction)
			throws StorageManagerException {
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		compactor.setMajorCompaction(majorCompaction);
		compactor.executeCompactation();
		writer.close();
		
		final SSTableReader reader = new SSTableReader(DATA_DIRECTORY, TEST_RELATION, 3);
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
		
		final SSTableWriter ssTableWriter = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, number);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		ssTableWriter.close();
		
		final SSTableReader sstableReader = new SSTableReader(DATA_DIRECTORY, TEST_RELATION, number);
		sstableReader.init();
		final SSTableKeyIndexReader ssTableIndexReader = new SSTableKeyIndexReader(sstableReader);
		ssTableIndexReader.init();
		
		return ssTableIndexReader;
	}
	
}

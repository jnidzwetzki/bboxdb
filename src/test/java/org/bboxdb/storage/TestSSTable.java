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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.sstable.reader.SSTableKeyIndexReader;
import org.bboxdb.storage.sstable.reader.SSTableReader;
import org.junit.Assert;
import org.junit.Test;

public class TestSSTable {
	
	/**
	 * The directory for the output
	 */
	protected static final String DATA_DIRECTORY = BBoxDBConfigurationManager.getConfiguration().getDataDirectory();
	
	/**
	 * The name of the test relation
	 */
	protected final static SSTableName TEST_RELATION = new SSTableName("1_testgroup1_relation3");
	
	/**
	 * The max number of expected tuples in the sstable
	 */
	protected final static int EXPECTED_TUPLES = 100;
	
	/**
	 * Test written files
	 * @throws Exception
	 */
	@Test
	public void testWrittenFiles() throws Exception {
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	
		final List<Tuple> tupleList = createTupleList();
		
		final SSTableWriter ssTableWriter = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 1, EXPECTED_TUPLES);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		final File sstableFile = ssTableWriter.getSstableFile();
		final File sstableIndexFile = ssTableWriter.getSstableIndexFile();
		ssTableWriter.close();
		
		Assert.assertTrue(sstableFile.exists());
		Assert.assertTrue(sstableIndexFile.exists());
	}
	
	/**
	 * Test the tuple iterator
	 * @throws Exception
	 */
	@Test
	public void testIndexIterator() throws Exception {
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	
		final List<Tuple> tupleList = createTupleList();
		
		final SSTableWriter ssTableWriter = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 1, EXPECTED_TUPLES);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		final File sstableIndexFile = ssTableWriter.getSstableIndexFile();
		ssTableWriter.close();
		
		final SSTableReader sstableReader = new SSTableReader(DATA_DIRECTORY, TEST_RELATION, 1);
		sstableReader.init();
		final SSTableKeyIndexReader ssTableIndexReader = new SSTableKeyIndexReader(sstableReader);
		ssTableIndexReader.init();
		
		Assert.assertEquals(1, sstableReader.getTablebumber());
		Assert.assertEquals(1, ssTableIndexReader.getTablebumber());
		Assert.assertEquals(sstableIndexFile, ssTableIndexReader.getFile());
		
		Assert.assertTrue(ssTableIndexReader.getSize() > 0);
		Assert.assertTrue(sstableReader.getSize() > 0);
		
		int tupleCounter = 0;
		
		for(Tuple tuple : ssTableIndexReader) {
			Assert.assertEquals(tupleList.get(tupleCounter), tuple);
			tupleCounter++;
		}
		
		Assert.assertEquals(tupleList.size(), tupleCounter);
	}

	/**
	 * Helper method for creating some test tuples
	 * 
	 * @return
	 */
	protected List<Tuple> createTupleList() {
		final List<Tuple> tupleList = new ArrayList<Tuple>();
		tupleList.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		tupleList.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		tupleList.add(new Tuple("3", BoundingBox.EMPTY_BOX, "geh".getBytes()));
		tupleList.add(new Tuple("4", BoundingBox.EMPTY_BOX, "ijk".getBytes()));
		tupleList.add(new DeletedTuple("4"));
		return tupleList;
	}
	
	/**
	 * Test delayed deletion
	 * @throws Exception
	 */
	@Test
	public void testDelayedDeletion() throws Exception {
		final SSTableManager storageManager = StorageRegistry.getSSTableManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	
		final String relationDirectory = SSTableHelper.getSSTableDir(DATA_DIRECTORY, TEST_RELATION);
		final File relationDirectoryFile = new File(relationDirectory);
		
		// Directory should be empty
		Assert.assertEquals(0, relationDirectoryFile.listFiles().length);
		
		final List<Tuple> tupleList = createTupleList();
		
		final SSTableWriter ssTableWriter = new SSTableWriter(DATA_DIRECTORY, TEST_RELATION, 1, EXPECTED_TUPLES);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		final File sstableFile = ssTableWriter.getSstableFile();
		final File sstableIndexFile = ssTableWriter.getSstableIndexFile();
		ssTableWriter.close();
		
		Assert.assertTrue(sstableFile.exists());
		Assert.assertTrue(sstableIndexFile.exists());
		
		final ReadOnlyTupleStorage ssTableFacade = new SSTableFacade(DATA_DIRECTORY, TEST_RELATION, 1);
		ssTableFacade.acquire();
		ssTableFacade.deleteOnClose();
		
		// Directory should contain at least the sstable, the key index and the meta data file
		Assert.assertTrue(relationDirectoryFile.listFiles().length >= 3);
		Assert.assertTrue(sstableFile.exists());
		Assert.assertTrue(sstableIndexFile.exists());
		
		// After calling release, the files can be deleted
		ssTableFacade.release();
		Assert.assertFalse(sstableFile.exists());
		Assert.assertFalse(sstableIndexFile.exists());
		
		// Directory should be empty
		Assert.assertEquals(0, relationDirectoryFile.listFiles().length);
	}
	
}

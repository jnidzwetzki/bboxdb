package de.fernunihagen.dna.jkn.scalephant.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableKeyIndexReader;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableReader;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableWriter;

public class TestSSTable {
	
	protected static final String DATA_DIRECTORY = ScalephantConfigurationManager.getConfiguration().getDataDirectory();
	
	protected final static String TEST_RELATION = "1_testgroup1_relation3";
	
	/**
	 * Test written files
	 * @throws Exception
	 */
	@Test
	public void testWrittenFiles() throws Exception {
		final StorageManager storageManager = StorageInterface.getStorageManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	
		final List<Tuple> tupleList = createTupleList();
		
		final SSTableWriter ssTableWriter = new SSTableWriter(TEST_RELATION, DATA_DIRECTORY, 1);
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
		final StorageManager storageManager = StorageInterface.getStorageManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	
		final List<Tuple> tupleList = createTupleList();
		
		final SSTableWriter ssTableWriter = new SSTableWriter(TEST_RELATION, DATA_DIRECTORY, 1);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		final File sstableFile = ssTableWriter.getSstableFile();
		final File sstableIndexFile = ssTableWriter.getSstableIndexFile();
		ssTableWriter.close();
		
		final SSTableReader sstableReader = new SSTableReader(TEST_RELATION, DATA_DIRECTORY, sstableFile);
		sstableReader.init();
		final SSTableKeyIndexReader ssTableIndexReader = new SSTableKeyIndexReader(sstableReader);
		ssTableIndexReader.init();
		
		Assert.assertEquals(sstableIndexFile, ssTableIndexReader.getFile());
		
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
		return tupleList;
	}
	
}

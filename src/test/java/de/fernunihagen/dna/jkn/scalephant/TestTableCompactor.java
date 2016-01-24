package de.fernunihagen.dna.jkn.scalephant;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageConfiguration;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManager;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableCompactor;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableIndexReader;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableReader;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableWriter;

public class TestTableCompactor {
	
	protected final static String TEST_RELATION = "testrelation";

	protected final StorageConfiguration storageConfiguration = new StorageConfiguration();

	@Before
	public void clearData() {
		final StorageManager storageManager = StorageInterface.getStorageManager(TEST_RELATION);
		storageManager.clear();
		storageManager.shutdown();
	}

	@Test
	public void testCompactTestFileCreation() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableIndexReader reader1 = addTuplesToFile(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableIndexReader reader2 = addTuplesToFile(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(storageConfiguration.getDataDir(), TEST_RELATION, 3);
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		boolean compactResult = compactor.executeCompactation();
		
		Assert.assertTrue(compactResult);
		Assert.assertTrue(writer.getSstableFile().exists());
		Assert.assertTrue(writer.getSstableIndexFile().exists());
		
		writer.close();
	}
	
	@Test
	public void testCompactTestMerge() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableIndexReader reader1 = addTuplesToFile(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableIndexReader reader2 = addTuplesToFile(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(storageConfiguration.getDataDir(), TEST_RELATION, 3);
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		compactor.executeCompactation();
		writer.close();
		
		final SSTableReader reader = new SSTableReader(TEST_RELATION, storageConfiguration.getDataDir(), 
				writer.getSstableFile());
		reader.init();
		
		final SSTableIndexReader ssTableIndexReader = new SSTableIndexReader(reader);
		ssTableIndexReader.init();
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			System.out.println(tuple);
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	
	@Test
	public void testCompactTestFileOneEmptyfile1() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableIndexReader reader1 = addTuplesToFile(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		final SSTableIndexReader reader2 = addTuplesToFile(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(storageConfiguration.getDataDir(), TEST_RELATION, 3);
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		compactor.executeCompactation();
		writer.close();
		
		final SSTableReader reader = new SSTableReader(TEST_RELATION, storageConfiguration.getDataDir(), 
				writer.getSstableFile());
		reader.init();
		
		final SSTableIndexReader ssTableIndexReader = new SSTableIndexReader(reader);
		ssTableIndexReader.init();
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			System.out.println(tuple);
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	@Test
	public void testCompactTestFileOneEmptyfile2() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		final SSTableIndexReader reader1 = addTuplesToFile(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableIndexReader reader2 = addTuplesToFile(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(storageConfiguration.getDataDir(), TEST_RELATION, 3);
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		compactor.executeCompactation();
		writer.close();
		
		final SSTableReader reader = new SSTableReader(TEST_RELATION, storageConfiguration.getDataDir(), 
				writer.getSstableFile());
		reader.init();
		
		final SSTableIndexReader ssTableIndexReader = new SSTableIndexReader(reader);
		ssTableIndexReader.init();
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			System.out.println(tuple);
		}
		
		Assert.assertEquals(tupleList1.size() + tupleList2.size(), counter);
	}
	
	@Test
	public void testCompactTestSameKey() throws StorageManagerException {
		final List<Tuple> tupleList1 = new ArrayList<Tuple>();
		tupleList1.add(new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes()));
		final SSTableIndexReader reader1 = addTuplesToFile(tupleList1, 1);
		
		final List<Tuple> tupleList2 = new ArrayList<Tuple>();
		tupleList2.add(new Tuple("1", BoundingBox.EMPTY_BOX, "def".getBytes()));
		final SSTableIndexReader reader2 = addTuplesToFile(tupleList2, 2);
		
		final SSTableWriter writer = new SSTableWriter(storageConfiguration.getDataDir(), TEST_RELATION, 3);
		
		final SSTableCompactor compactor = new SSTableCompactor(Arrays.asList(reader1, reader2), writer);
		compactor.executeCompactation();
		writer.close();
		
		final SSTableReader reader = new SSTableReader(TEST_RELATION, storageConfiguration.getDataDir(), 
				writer.getSstableFile());
		reader.init();
		
		final SSTableIndexReader ssTableIndexReader = new SSTableIndexReader(reader);
		ssTableIndexReader.init();
		
		int counter = 0;
		for(final Tuple tuple : ssTableIndexReader) {
			counter++;
			Assert.assertEquals("def", new String(tuple.getDataBytes()));
		}		
				
		Assert.assertEquals(1, counter);
	}
	
	
	protected SSTableIndexReader addTuplesToFile(final List<Tuple> tupleList, int number)
			throws StorageManagerException {
		final SSTableWriter ssTableWriter = new SSTableWriter(storageConfiguration.getDataDir(), TEST_RELATION, number);
		ssTableWriter.open();
		ssTableWriter.addData(tupleList);
		final File sstableFile = ssTableWriter.getSstableFile();
		ssTableWriter.close();
		
		final SSTableReader sstableReader = new SSTableReader(TEST_RELATION, storageConfiguration.getDataDir(), sstableFile);
		sstableReader.init();
		final SSTableIndexReader ssTableIndexReader = new SSTableIndexReader(sstableReader);
		ssTableIndexReader.init();
		
		return ssTableIndexReader;
	}
	
}

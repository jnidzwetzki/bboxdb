package de.fernunihagen.dna.jkn.scalephant.storage;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SStableMetaData;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableMetadataBuilder;

public class TestSSTableMetadataBuilder {

	/**
	 * Build empty index
	 */
	@Test
	public void testSSTableIndexBuilder1() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new float[] {}, metadata.getBoundingBoxData(), 0.001f);
	}

	/**
	 * Build index with one tuple
	 */
	@Test
	public void testSSTableIndexBuilder2() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		
		ssTableIndexBuilder.addTuple(tuple1);
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox1.toFloatArray(), metadata.getBoundingBoxData(), 0.001f);
		Assert.assertEquals(metadata.getOldestTuple(), metadata.getNewestTuple());
	}
	
	/**
	 * Build index with two tuples
	 */
	@Test
	public void testSSTableIndexBuilder3() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final Tuple tuple2 = new Tuple("def", boundingBox1, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox1.toFloatArray(), metadata.getBoundingBoxData(), 0.001f);
	}
	
	/**
	 * Build index with two tuples
	 */
	@Test
	public void testSSTableIndexBuilder4() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1f, 2f);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox2.toFloatArray(), metadata.getBoundingBoxData(), 0.001f);
	}
	
	/**
	 * Build index with two tuples
	 */
	@Test
	public void testSSTableIndexBuilder5() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1f, 0.1f);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox1.toFloatArray(), metadata.getBoundingBoxData(), 0.001f);
	}
	
	/**
	 * Build index with two tuples - bounding boxes are differ in the dimension
	 */
	@Test
	public void testSSTableIndexBuilder6() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f, 1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1f, 0.1f);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new float[] {}, metadata.getBoundingBoxData(), 0.001f);
	}
	
	
	/**
	 * Build index with two tuples - bounding boxes are differ in the dimension
	 */
	@Test
	public void testSSTableIndexBuilder7() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		addTwoTuples(ssTableIndexBuilder);
		
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new float[] {1f, 1f, 1f, 4f}, metadata.getBoundingBoxData(), 0.001f);
	}
	
	/**
	 * Dump the index to yaml
	 */
	@Test
	public void testDumpToYaml1() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		final String yamlData = ssTableIndexBuilder.getMetaData().exportToYaml();
		Assert.assertTrue(yamlData.length() > 10);
	}
	
	/**
	 * Dump the index to yaml
	 */
	@Test
	public void testDumpToYaml2() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f, 1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		ssTableIndexBuilder.addTuple(tuple1);

		final String yamlData = ssTableIndexBuilder.getMetaData().exportToYaml();
		Assert.assertTrue(yamlData.length() > 10);
	}
	
	/**
	 * Dump the index to yaml and reread the data
	 */
	@Test
	public void testDumpAndReadFromYamlString1() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f, 1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		ssTableIndexBuilder.addTuple(tuple1);

		final SStableMetaData metaData = ssTableIndexBuilder.getMetaData();
		final String yamlData = metaData.exportToYaml();
				
		final SStableMetaData metaDataRead = SStableMetaData.importFromYaml(yamlData);
		Assert.assertEquals(metaData, metaDataRead);
	}
	
	/**
	 * Dump the index to yaml and reread the data
	 */
	@Test
	public void testDumpAndReadFromYamlStrig2() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		addTwoTuples(ssTableIndexBuilder);

		final SStableMetaData metaData = ssTableIndexBuilder.getMetaData();
		final String yamlData = metaData.exportToYaml();
			
		final SStableMetaData metaDataRead = SStableMetaData.importFromYaml(yamlData);
		Assert.assertEquals(metaData, metaDataRead);
	}
	
	/**
	 * Dump the index to yaml file and reread the data - zero tuples
	 * @throws IOException 
	 */
	@Test
	public void testDumpAndReadFromYamlFile1() throws IOException {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
				
		final File tmpFile = File.createTempFile("test", ".tmp");

		final SStableMetaData metaData = ssTableIndexBuilder.getMetaData();
		metaData.exportToYamlFile(tmpFile);
			
		final SStableMetaData metaDataRead = SStableMetaData.importFromYamlFile(tmpFile);
		Assert.assertEquals(metaData, metaDataRead);
		tmpFile.delete();
	}

	
	/**
	 * Dump the index to yaml file and reread the data - two tuple
	 * @throws IOException 
	 */
	@Test
	public void testDumpAndReadFromYamlFile2() throws IOException {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		addTwoTuples(ssTableIndexBuilder);
		
		final File tmpFile = File.createTempFile("test", ".tmp");

		final SStableMetaData metaData = ssTableIndexBuilder.getMetaData();
		metaData.exportToYamlFile(tmpFile);
			
		final SStableMetaData metaDataRead = SStableMetaData.importFromYamlFile(tmpFile);
		Assert.assertEquals(metaData, metaDataRead);
		tmpFile.delete();
	}

	/**
	 * Add two tuples to the index builder
	 * @param ssTableIndexBuilder
	 */
	protected void addTwoTuples(final SSTableMetadataBuilder ssTableIndexBuilder) {
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f, 1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1f, 0.1f, 1f, 4f);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());
		
		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
	}
	
}

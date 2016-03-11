package de.fernunihagen.dna.jkn.scalephant.storage;

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
		
		final BoundingBox boundingBox1 = new BoundingBox(1f, 1f, 1f, 1f);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1f, 0.1f, 1f, 4f);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final SStableMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new float[] {1f, 1f, 1f, 4f}, metadata.getBoundingBoxData(), 0.001f);
	}
	
}

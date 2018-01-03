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
import java.io.IOException;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreMetaData;
import org.bboxdb.storage.sstable.SSTableMetadataBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestSSTableMetadataBuilder {

	/**
	 * Build empty index
	 */
	@Test
	public void testSSTableIndexBuilder1() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new double[] {}, metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(0, metadata.getTuples());
	}

	/**
	 * Build index with one tuple
	 */
	@Test
	public void testSSTableIndexBuilder2() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		
		ssTableIndexBuilder.addTuple(tuple1);
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox1.toDoubleArray(), metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(metadata.getOldestTupleVersionTimestamp(), metadata.getNewestTupleVersionTimestamp());
		Assert.assertEquals(1, metadata.getTuples());
		Assert.assertEquals(tuple1.getReceivedTimestamp(), metadata.getNewestTupleInsertedTimstamp());
	}
	
	/**
	 * Build index with two tuples
	 */
	@Test
	public void testSSTableIndexBuilder3() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final Tuple tuple2 = new Tuple("def", boundingBox1, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox1.toDoubleArray(), metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(2, metadata.getTuples());
	}
	
	/**
	 * Build index with two tuples
	 */
	@Test
	public void testSSTableIndexBuilder4() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1d, 3d);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox2.toDoubleArray(), metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(2, metadata.getTuples());
	}
	
	/**
	 * Build index with two tuples
	 */
	@Test
	public void testSSTableIndexBuilder5() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1d, 1.1d);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(boundingBox1.toDoubleArray(), metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(2, metadata.getTuples());
	}
	
	/**
	 * Build index with two tuples - bounding boxes are differ in the dimension
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testSSTableIndexBuilder6() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1d, 1.1d);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());

		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
		
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new double[] {}, metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(2, metadata.getTuples());
	}
	
	
	/**
	 * Build index with two tuples - bounding boxes are differ in the dimension
	 */
	@Test
	public void testSSTableIndexBuilder7() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		addTwoTuples(ssTableIndexBuilder);
		
		final TupleStoreMetaData metadata = ssTableIndexBuilder.getMetaData();
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 5f}, metadata.getBoundingBoxData(), 0.001d);
		Assert.assertEquals(2, metadata.getTuples());
	}
	
	
	/**
	 * Build index with multiple tuples - check timestamps
	 */
	@Test
	public void testSSTableIndexBuilder8() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		final Tuple tuple1 = new Tuple("0", BoundingBox.EMPTY_BOX, "".getBytes(), 6);
		ssTableIndexBuilder.addTuple(tuple1);
		Assert.assertEquals(6, ssTableIndexBuilder.getMetaData().getNewestTupleVersionTimestamp());
		Assert.assertEquals(6, ssTableIndexBuilder.getMetaData().getOldestTupleVersionTimestamp());
		Assert.assertEquals(tuple1.getReceivedTimestamp(), ssTableIndexBuilder.getMetaData().getNewestTupleInsertedTimstamp());

		final Tuple tuple2 = new Tuple("0", BoundingBox.EMPTY_BOX, "".getBytes(), 7);
		ssTableIndexBuilder.addTuple(tuple2);
		Assert.assertEquals(7, ssTableIndexBuilder.getMetaData().getNewestTupleVersionTimestamp());
		Assert.assertEquals(6, ssTableIndexBuilder.getMetaData().getOldestTupleVersionTimestamp());
		Assert.assertEquals(tuple2.getReceivedTimestamp(), ssTableIndexBuilder.getMetaData().getNewestTupleInsertedTimstamp());

		final Tuple tuple3 = new Tuple("0", BoundingBox.EMPTY_BOX, "".getBytes(), 2);
		ssTableIndexBuilder.addTuple(tuple3);
		Assert.assertEquals(7, ssTableIndexBuilder.getMetaData().getNewestTupleVersionTimestamp());
		Assert.assertEquals(2, ssTableIndexBuilder.getMetaData().getOldestTupleVersionTimestamp());
		Assert.assertEquals(tuple3.getReceivedTimestamp(), ssTableIndexBuilder.getMetaData().getNewestTupleInsertedTimstamp());

		final Tuple tuple4 = new DeletedTuple("0", 22);
		ssTableIndexBuilder.addTuple(tuple4);
		Assert.assertEquals(22, ssTableIndexBuilder.getMetaData().getNewestTupleVersionTimestamp());
		Assert.assertEquals(2, ssTableIndexBuilder.getMetaData().getOldestTupleVersionTimestamp());
		Assert.assertEquals(tuple4.getReceivedTimestamp(), ssTableIndexBuilder.getMetaData().getNewestTupleInsertedTimstamp());
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
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 2d);
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
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		ssTableIndexBuilder.addTuple(tuple1);

		final TupleStoreMetaData metaData = ssTableIndexBuilder.getMetaData();
		final String yamlData = metaData.exportToYaml();
				
		final TupleStoreMetaData metaDataRead = TupleStoreMetaData.importFromYaml(yamlData);
		Assert.assertEquals(metaData, metaDataRead);
	}
	
	/**
	 * Dump the index to yaml and reread the data
	 */
	@Test
	public void testDumpAndReadFromYamlStrig2() {
		final SSTableMetadataBuilder ssTableIndexBuilder = new SSTableMetadataBuilder();
		
		addTwoTuples(ssTableIndexBuilder);

		final TupleStoreMetaData metaData = ssTableIndexBuilder.getMetaData();
		final String yamlData = metaData.exportToYaml();
			
		final TupleStoreMetaData metaDataRead = TupleStoreMetaData.importFromYaml(yamlData);
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

		final TupleStoreMetaData metaData = ssTableIndexBuilder.getMetaData();
		metaData.exportToYamlFile(tmpFile);
			
		final TupleStoreMetaData metaDataRead = TupleStoreMetaData.importFromYamlFile(tmpFile);
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

		final TupleStoreMetaData metaData = ssTableIndexBuilder.getMetaData();
		metaData.exportToYamlFile(tmpFile);
			
		final TupleStoreMetaData metaDataRead = TupleStoreMetaData.importFromYamlFile(tmpFile);
		Assert.assertEquals(metaData, metaDataRead);
		tmpFile.delete();
	}

	/**
	 * Add two tuples to the index builder
	 * @param ssTableIndexBuilder
	 */
	protected void addTwoTuples(final SSTableMetadataBuilder ssTableIndexBuilder) {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 2d);
		final Tuple tuple1 = new Tuple("abc", boundingBox1, "".getBytes());
		final BoundingBox boundingBox2 = new BoundingBox(1d, 1.1d, 1d, 5d);
		final Tuple tuple2 = new Tuple("def", boundingBox2, "".getBytes());
		
		ssTableIndexBuilder.addTuple(tuple1);
		ssTableIndexBuilder.addTuple(tuple2);
	}
	
}

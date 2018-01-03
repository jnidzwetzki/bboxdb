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
package org.bboxdb.storage.rtree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestRTreeIndex {
	
	/**
	 * Test to insert and to read the bounding boxes
	 */
	@Test
	public void testBoxesInsert() {
		final List<SpatialIndexEntry> elements = RTreeTestHelper.getEntryList();
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(elements);
	}
	
	@Test
	public void testQueryOnEmptytree() {
		final SpatialIndexBuilder index = new RTreeBuilder();
		final List<? extends SpatialIndexEntry> result = index.getEntriesForRegion(new BoundingBox(1d, 1d, 2d, 2d));
		Assert.assertTrue(result.isEmpty());
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery1d() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.getEntryList();
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		RTreeTestHelper.queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery2d() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(2);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		RTreeTestHelper.queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery3d() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(3);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery4d() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(4);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery5d() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(5);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery10d() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(10);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
	}
	
	
	/**
	 * Test the covering of the nodes
	 */
	@Test
	public void testCovering() {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(3);
		
		final RTreeBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		index.testCovering();		
	}
	
	/**
	 * Test the decoding an encoding of an rtree entry
	 * @throws IOException 
	 */
	@Test
	public void testEncodeDecodeRTreeEntryFromFile() throws IOException {
		final BoundingBox boundingBox = new BoundingBox(4.1, 8.1, 4.2, 8.8);
		
		final File tempFile = File.createTempFile("rtree-", "-test");
		tempFile.deleteOnExit();
		final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");		
		final SpatialIndexEntry rTreeSpatialIndexEntry = new SpatialIndexEntry(boundingBox, 1);
		rTreeSpatialIndexEntry.writeToFile(raf);		
		raf.close();
		
		final RandomAccessFile rafRead = new RandomAccessFile(tempFile, "r");
		final SpatialIndexEntry readEntry = SpatialIndexEntry.readFromFile(rafRead);
		rafRead.close();
		
		Assert.assertEquals(rTreeSpatialIndexEntry.getValue(), readEntry.getValue());
		Assert.assertEquals(rTreeSpatialIndexEntry.getBoundingBox(), readEntry.getBoundingBox());
	}
	
	/**
	 * Test the decoding an encoding of an rtree entry
	 * @throws IOException 
	 */
	@Test
	public void testEncodeDecodeRTreeEntryFromByteBuffer() throws IOException {
		final BoundingBox boundingBox = new BoundingBox(4.1, 8.1, 4.2, 8.8);
		
		final File tempFile = File.createTempFile("rtree-", "-test");
		tempFile.deleteOnExit();
		final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");		
		final SpatialIndexEntry rTreeSpatialIndexEntry = new SpatialIndexEntry(boundingBox, 1);
		rTreeSpatialIndexEntry.writeToFile(raf);		
		raf.close();
		
		final Path path = Paths.get(tempFile.getAbsolutePath());
		final byte[] data = Files.readAllBytes(path);
		final ByteBuffer bb = ByteBuffer.wrap(data);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		
		final SpatialIndexEntry readEntry = SpatialIndexEntry.readFromByteBuffer(bb);
		
		Assert.assertEquals(rTreeSpatialIndexEntry.getValue(), readEntry.getValue());
		Assert.assertEquals(rTreeSpatialIndexEntry.getBoundingBox(), readEntry.getBoundingBox());
	}

	/**
	 * Test the creation of a rtree with a invalid max node size
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testWrongNodeSize0() {
		new RTreeBuilder(0);
	}
	
	/**
	 * Test the creation of a rtree with a invalid max node size
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testWrongNodeSize1() {
		new RTreeBuilder(-1);
	}
	
	/**
	 * Test the bounding box of an empty r-tree
	 */
	@Test
	public void testEmptryRTreeBBox() {
		final RTreeBuilder index = new RTreeBuilder();
		final List<? extends SpatialIndexEntry> result = index.getEntriesForRegion(BoundingBox.EMPTY_BOX);
		Assert.assertTrue(result.isEmpty());
	}
	
}

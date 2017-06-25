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
package org.bboxdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexReader;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeSpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeSpatialIndexMemoryReader;
import org.junit.Assert;
import org.junit.Test;

public class TestSpatialRTreeIndex {
	
	/**
	 * Test to insert and to read the bounding boxes
	 */
	@Test
	public void testBoxesInsert() {
		final List<SpatialIndexEntry> elements = getEntryList();
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(elements);
	}
	
	@Test
	public void testQueryOnEmptytree() {
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		final List<? extends SpatialIndexEntry> result = index.getEntriesForRegion(new BoundingBox(1d, 1d, 2d, 2d));
		Assert.assertTrue(result.isEmpty());
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery1d() {
		final List<SpatialIndexEntry> tupleList = getEntryList();
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery2d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(2);
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery3d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(3);
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery4d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(4);
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery5d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(5);
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery10d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(10);
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	
	/**
	 * Test the covering of the nodes
	 */
	@Test
	public void testCovering() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(3);
		
		final RTreeSpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		
		index.testCovering();		
	}
	
	/**
	 * Test the query
	 * 
	 * @param entries
	 * @param index
	 */
	protected void queryIndex(final List<SpatialIndexEntry> entries, final SpatialIndexReader index) {
		
		for(final SpatialIndexEntry entry: entries) {
			final List<? extends SpatialIndexEntry> resultList = index.getEntriesForRegion(entry.getBoundingBox());
			Assert.assertTrue(resultList.size() >= 1);
			
			final List<Long> keyResult = resultList
					.stream()
					.map(e -> e.getValue())
					.filter(k -> k.equals(entry.getValue()))
					.collect(Collectors.toList());

			Assert.assertTrue("Searching for: " + entry, keyResult.size() == 1);
		}
	}

	/**
	 * Test the query
	 * 
	 * @param entries
	 * @param index
	 */
	protected void queryIndex(final List<SpatialIndexEntry> entries, final SpatialIndexBuilder index) {
		
		for(final SpatialIndexEntry entry: entries) {
			final List<? extends SpatialIndexEntry> resultList = index.getEntriesForRegion(entry.getBoundingBox());
			Assert.assertTrue(resultList.size() >= 1);
			
			final List<Long> keyResult = resultList
					.stream()
					.map(e -> e.getValue())
					.filter(k -> k.equals(entry.getValue()))
					.collect(Collectors.toList());

			Assert.assertTrue("Searching for: " + entry, keyResult.size() == 1);
		}
	}

	/**
	 * Generate a list of tuples
	 * @return
	 */
	protected List<SpatialIndexEntry> getEntryList() {
		final List<SpatialIndexEntry> entryList = new ArrayList<SpatialIndexEntry>();
		entryList.add(new SpatialIndexEntry(new BoundingBox(0d, 1d, 0d, 1d), 1));
		entryList.add(new SpatialIndexEntry(new BoundingBox(1d, 2d, 1d, 3d), 2));
		entryList.add(new SpatialIndexEntry(new BoundingBox(2d, 3d, 0d, 1d), 3));
		entryList.add(new SpatialIndexEntry(new BoundingBox(3d, 4d, 3d, 7d), 4));
		entryList.add(new SpatialIndexEntry(new BoundingBox(1.2d, 2.2d, 0d, 1d), 5));
		entryList.add(new SpatialIndexEntry(new BoundingBox(4.6d, 5.6d, 0d, 1d), 6));
		entryList.add(new SpatialIndexEntry(new BoundingBox(5.2d, 6.2d, 4d, 5d), 7));
		entryList.add(new SpatialIndexEntry(new BoundingBox(5.1d, 6.1d, 0d, 1d), 8));
		entryList.add(new SpatialIndexEntry(new BoundingBox(6.1d, 7.1d, 0d, 1d), 9));
		entryList.add(new SpatialIndexEntry(new BoundingBox(8.1d, 9.1d, 2d, 5d), 10));
		return entryList;
	}
	
	/**
	 * Generate some random tuples
	 * @return
	 */
	protected List<SpatialIndexEntry> generateRandomTupleList(final int dimensions) {
		final List<SpatialIndexEntry> entryList = new ArrayList<SpatialIndexEntry>();
		final Random random = new Random();
		
		for(int i = 0; i < 5000; i++) {
			final double[] boundingBoxData = new double[dimensions * 2];
			
			for(int d = 0; d < dimensions; d++) {
				final double begin = random.nextInt() % 1000;
				final double extent = Math.abs(random.nextInt() % 1000);
				boundingBoxData[2 * d] = begin;            // Start coordinate
				boundingBoxData[2 * d + 1] = begin+extent; // End coordinate
			}
			
			final SpatialIndexEntry entry = new SpatialIndexEntry(new BoundingBox(boundingBoxData), i);
			entryList.add(entry);
		}

		return entryList;
	}
	
	/**
	 * Test the decoding an encoding of an rtree entry
	 * @throws IOException 
	 */
	@Test
	public void testEncodeDecodeRTreeEntry() throws IOException {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		final BoundingBox boundingBox = new BoundingBox(4.1, 8.1, 4.2, 8.8);
		
		final SpatialIndexEntry rTreeSpatialIndexEntry = new SpatialIndexEntry(boundingBox, 1);
		rTreeSpatialIndexEntry.writeToStream(bos);
		bos.close();
		
		final byte[] bytes = bos.toByteArray();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		
		Assert.assertTrue(bis.available() > 0);
		final SpatialIndexEntry readEntry = SpatialIndexEntry.readFromStream(bis);
		Assert.assertTrue(bis.available() == 0);

		bis.close();
		
		Assert.assertEquals(rTreeSpatialIndexEntry.getValue(), readEntry.getValue());
		Assert.assertEquals(rTreeSpatialIndexEntry.getBoundingBox(), readEntry.getBoundingBox());
	}

	/**
	 * Test the creation of a rtree with a invalid max node size
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testWrongNodeSize0() {
		new RTreeSpatialIndexBuilder(0);
	}
	
	/**
	 * Test the creation of a rtree with a invalid max node size
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testWrongNodeSize1() {
		new RTreeSpatialIndexBuilder(-1);
	}
	
	/**
	 * Test different node size
	 * @throws StorageManagerException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testSerializeIndex0() throws StorageManagerException, IOException, InterruptedException {
		final int maxNodeSize = 12;
		final RTreeSpatialIndexBuilder index = new RTreeSpatialIndexBuilder(maxNodeSize);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		index.writeToStream(bos);
		bos.close();
		
		final byte[] bytes = bos.toByteArray();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		final RTreeSpatialIndexMemoryReader indexRead = new RTreeSpatialIndexMemoryReader();
		indexRead.readFromStream(bis);

		Assert.assertEquals(maxNodeSize, index.getMaxNodeSize());
		Assert.assertEquals(maxNodeSize, indexRead.getMaxNodeSize());
	}
	
	/**
	 * Test the encoding and the decoding of the index
	 * @throws StorageManagerException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSerializeIndex1() throws StorageManagerException, IOException, InterruptedException {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(3);
		
		final SpatialIndexBuilder index = new RTreeSpatialIndexBuilder();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		index.writeToStream(bos);
		bos.close();
		
		final byte[] bytes = bos.toByteArray();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		final RTreeSpatialIndexMemoryReader indexRead = new RTreeSpatialIndexMemoryReader();
		
		Assert.assertTrue(bis.available() > 0);
		indexRead.readFromStream(bis);
		Assert.assertTrue(bis.available() == 0);
		
		bis.close();
		
		queryIndex(tupleList, indexRead);
	}
	
}

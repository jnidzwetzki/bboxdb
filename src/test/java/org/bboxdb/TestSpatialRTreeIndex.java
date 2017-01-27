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
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexStrategy;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeSpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeSpatialIndexStrategy;
import org.junit.Assert;
import org.junit.Test;

public class TestSpatialRTreeIndex {
	
	/**
	 * Test to insert and to read the bounding boxes
	 */
	@Test
	public void testBoxesInsert() {
		final List<SpatialIndexEntry> elements = getEntryList();
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(elements);
	}
	
	@Test
	public void testQueryOnEmptytree() {
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		final List<? extends SpatialIndexEntry> result = index.getEntriesForRegion(new BoundingBox(1d, 1d, 2d, 2d));
		Assert.assertTrue(result.isEmpty());
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery1d() {
		final List<SpatialIndexEntry> tupleList = getEntryList();
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery2d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(2);
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery3d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(3);
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery4d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(4);
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery5d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(5);
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery10d() {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(10);
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
	}

	/**
	 * Test the query
	 * 
	 * @param entries
	 * @param index
	 */
	protected void queryIndex(final List<SpatialIndexEntry> entries, final SpatialIndexStrategy index) {
		
		for(final SpatialIndexEntry entry: entries) {
			final List<? extends SpatialIndexEntry> resultList = index.getEntriesForRegion(entry.getBoundingBox());
			Assert.assertTrue(resultList.size() >= 1);
			
			final List<String> keyResult = resultList
					.stream()
					.map(e -> e.getKey())
					.filter(k -> k.equals(entry.getKey()))
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
		entryList.add(new SpatialIndexEntry("abc", new BoundingBox(0d, 1d, 0d, 1d)));
		entryList.add(new SpatialIndexEntry("def", new BoundingBox(1d, 2d, 1d, 3d)));
		entryList.add(new SpatialIndexEntry("fgh", new BoundingBox(2d, 3d, 0d, 1d)));
		entryList.add(new SpatialIndexEntry("ijk", new BoundingBox(3d, 4d, 3d, 7d)));
		entryList.add(new SpatialIndexEntry("lmn", new BoundingBox(1.2d, 2.2d, 0d, 1d)));
		entryList.add(new SpatialIndexEntry("ijk", new BoundingBox(4.6d, 5.6d, 0d, 1d)));
		entryList.add(new SpatialIndexEntry("dwe", new BoundingBox(5.2d, 6.2d, 4d, 5d)));
		entryList.add(new SpatialIndexEntry("gwd", new BoundingBox(5.1d, 6.1d, 0d, 1d)));
		entryList.add(new SpatialIndexEntry("fs3", new BoundingBox(6.1d, 7.1d, 0d, 1d)));
		entryList.add(new SpatialIndexEntry("xyz", new BoundingBox(8.1d, 9.1d, 2d, 5d)));
		return entryList;
	}
	
	/**
	 * Generate some random tuples
	 * @return
	 */
	protected List<SpatialIndexEntry> generateRandomTupleList(int dimensions) {
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
			
			final SpatialIndexEntry entry = new SpatialIndexEntry(Integer.toBinaryString(i), new BoundingBox(boundingBoxData));
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
		
		final RTreeSpatialIndexEntry rTreeSpatialIndexEntry = new RTreeSpatialIndexEntry(3, "abc", boundingBox);
		rTreeSpatialIndexEntry.writeToStream(bos);
		bos.close();
		
		final byte[] bytes = bos.toByteArray();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		
		Assert.assertTrue(bis.available() > 0);
		final RTreeSpatialIndexEntry readEntry = RTreeSpatialIndexEntry.readFromStream(bis);
		Assert.assertTrue(bis.available() == 0);

		bis.close();
		
		Assert.assertEquals(rTreeSpatialIndexEntry.getKey(), readEntry.getKey());
		Assert.assertEquals(rTreeSpatialIndexEntry.getNodeId(), readEntry.getNodeId());
		Assert.assertEquals(rTreeSpatialIndexEntry.getBoundingBox(), readEntry.getBoundingBox());
	}

	/**
	 * Test the encoding and the decoding of the index
	 * @throws StorageManagerException 
	 * @throws IOException 
	 */
	@Test
	public void testSerializeIndex() throws StorageManagerException, IOException {
		final List<SpatialIndexEntry> tupleList = generateRandomTupleList(3);
		
		final SpatialIndexStrategy index = new RTreeSpatialIndexStrategy();
		index.bulkInsert(tupleList);
		
		queryIndex(tupleList, index);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		index.writeToStream(bos);
		bos.close();
		
		final byte[] bytes = bos.toByteArray();
		final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		RTreeSpatialIndexStrategy indexRead = new RTreeSpatialIndexStrategy();
		
		Assert.assertTrue(bis.available() > 0);
		indexRead.readFromStream(bis);
		Assert.assertTrue(bis.available() == 0);
		
		bis.close();
		
		queryIndex(tupleList, indexRead);
	}
	
}

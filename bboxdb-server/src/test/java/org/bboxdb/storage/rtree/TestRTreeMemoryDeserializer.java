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
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.AbstractRTreeReader;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeMemoryReader;
import org.junit.Assert;
import org.junit.Test;

public class TestRTreeMemoryDeserializer {

	/**
	 * Get the reader for the test
	 * @return
	 */
	protected AbstractRTreeReader getRTreeReader() {
		return new RTreeMemoryReader();
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
		final RTreeBuilder index = new RTreeBuilder(maxNodeSize);
		
		final File tempFile = File.createTempFile("rtree-", "-test");
		tempFile.deleteOnExit();
		final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");		
		index.writeToFile(raf);
		raf.close();
			
		final AbstractRTreeReader indexRead = getRTreeReader();
		final RandomAccessFile rafRead = new RandomAccessFile(tempFile, "r");
		indexRead.readFromFile(rafRead);
		rafRead.close();

		Assert.assertEquals(maxNodeSize, index.getMaxNodeSize());
		Assert.assertEquals(maxNodeSize, indexRead.getMaxNodeSize());
		
		indexRead.close();
	}
	
	/**
	 * Test the encoding and the decoding of the index with only one entry 
	 * = data is encoded in the root node
	 * 
	 * @throws StorageManagerException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSerializeIndexSmall() throws StorageManagerException, IOException, InterruptedException {
		final List<SpatialIndexEntry> tupleList = new ArrayList<>();
		tupleList.add(new SpatialIndexEntry(new BoundingBox(1.0, 1.2), 2));
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
		
		final File tempFile = File.createTempFile("rtree-", "-test");
		tempFile.deleteOnExit();
		final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");		
		index.writeToFile(raf);
		raf.close();
		
		final AbstractRTreeReader indexRead = getRTreeReader();
		final RandomAccessFile rafRead = new RandomAccessFile(tempFile, "r");
		indexRead.readFromFile(rafRead);
		rafRead.close();
		
		final List<? extends SpatialIndexEntry> resultList = indexRead.getEntriesForRegion(new BoundingBox(1.1, 1.2));
		Assert.assertEquals(1, resultList.size());
		
		indexRead.close();
	}
	
	/**
	 * Test the encoding and the decoding of the index
	 * @throws StorageManagerException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSerializeIndex1D() throws StorageManagerException, IOException, InterruptedException {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(1);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
		
		final File tempFile = File.createTempFile("rtree-", "-test");
		tempFile.deleteOnExit();
		final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");		
		index.writeToFile(raf);
		raf.close();
		
		final AbstractRTreeReader indexRead = getRTreeReader();
		final RandomAccessFile rafRead = new RandomAccessFile(tempFile, "r");
		indexRead.readFromFile(rafRead);
		rafRead.close();
		
		RTreeTestHelper.queryIndex(tupleList, indexRead);
	}


	/**
	 * Test the encoding and the decoding of the index
	 * @throws StorageManagerException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSerializeIndex3D() throws StorageManagerException, IOException, InterruptedException {
		final List<SpatialIndexEntry> tupleList = RTreeTestHelper.generateRandomTupleList(3);
		
		final SpatialIndexBuilder index = new RTreeBuilder();
		index.bulkInsert(tupleList);
		
		RTreeTestHelper.queryIndex(tupleList, index);
		
		final File tempFile = File.createTempFile("rtree-", "-test");
		tempFile.deleteOnExit();
		final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");		
		index.writeToFile(raf);
		raf.close();
		
		final AbstractRTreeReader indexRead = getRTreeReader();
		final RandomAccessFile rafRead = new RandomAccessFile(tempFile, "r");
		indexRead.readFromFile(rafRead);
		rafRead.close();
		
		RTreeTestHelper.queryIndex(tupleList, indexRead);
	}
	

}

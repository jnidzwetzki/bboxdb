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
package org.bboxdb.test.storage.rtree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexReader;
import org.junit.Assert;

public class RTreeTestHelper {

	/**
	 * Test the query
	 * 
	 * @param entries
	 * @param index
	 * @throws StorageManagerException 
	 */
	public static void queryIndex(final List<SpatialIndexEntry> entries, final SpatialIndexReader index) throws StorageManagerException {
		
		for(final SpatialIndexEntry entry: entries) {
			final List<? extends SpatialIndexEntry> resultList 
				= index.getEntriesForRegion(entry.getBoundingBox());
			
			checkResult(entry, resultList);
		}
	}

	/**
	 * Test the query
	 * 
	 * @param entries
	 * @param index
	 */
	public static void queryIndex(final List<SpatialIndexEntry> entries, final SpatialIndexBuilder index) {
		
		for(final SpatialIndexEntry entry: entries) {
			final List<? extends SpatialIndexEntry> resultList 
				= index.getEntriesForRegion(entry.getBoundingBox());
			
			checkResult(entry, resultList);
		}
	}

	/**
	 * Check the result list
	 * @param entry
	 * @param resultList
	 */
	private static void checkResult(final SpatialIndexEntry entry, 
			final List<? extends SpatialIndexEntry> resultList) {
		
		final List<Integer> keyResult = resultList
				.stream()
				.map(e -> e.getValue())
				.filter(k -> k.equals(entry.getValue()))
				.collect(Collectors.toList());

		Assert.assertTrue("Searching for: " + entry.getValue(), keyResult.size() == 1);
		
		// Manual search
		final AtomicInteger counter = new AtomicInteger(0);
		for(SpatialIndexEntry element : resultList) {
			if(element.getBoundingBox().intersects(entry.getBoundingBox())) {
				counter.incrementAndGet();
			}
		}
		
		Assert.assertEquals(resultList.size(), counter.get());
	}

	/**
	 * Generate a list of tuples
	 * @return
	 */
	public static List<SpatialIndexEntry> getEntryList() {
		final List<SpatialIndexEntry> entryList = new ArrayList<>();
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(0d, 1d, 0d, 1d), 1));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(1d, 2d, 1d, 3d), 2));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(2d, 3d, 0d, 1d), 3));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(3d, 4d, 3d, 7d), 4));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(1.2d, 2.2d, 0d, 1d), 5));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(4.6d, 5.6d, 0d, 1d), 6));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(5.2d, 6.2d, 4d, 5d), 7));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(5.1d, 6.1d, 0d, 1d), 8));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(6.1d, 7.1d, 0d, 1d), 9));
		entryList.add(new SpatialIndexEntry(new Hyperrectangle(8.1d, 9.1d, 2d, 5d), 10));
		return entryList;
	}
	
	/**
	 * Generate some random tuples
	 * @return
	 */
	public static List<SpatialIndexEntry> generateRandomTupleList(final int dimensions, final int elements) {
		final List<SpatialIndexEntry> entryList = new ArrayList<>();
		final ThreadLocalRandom random = ThreadLocalRandom.current();
		
		for(int i = 0; i < elements; i++) {
			final double[] boundingBoxData = new double[dimensions * 2];
			
			for(int d = 0; d < dimensions; d++) {
				final double begin = random.nextDouble(-10000, 10000);
				final double extent = random.nextDouble(0, 50);
				boundingBoxData[2 * d] = begin;            // Start coordinate
				boundingBoxData[2 * d + 1] = begin+extent; // End coordinate
			}
			
			final Hyperrectangle bbox = new Hyperrectangle(boundingBoxData);
			final SpatialIndexEntry entry = new SpatialIndexEntry(bbox, i);
			entryList.add(entry);
		}

		return entryList;
	}
}

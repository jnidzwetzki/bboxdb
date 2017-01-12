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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.spatialindex.BoxSortSpatialIndexStrategy;
import org.junit.Assert;
import org.junit.Test;

public class TestBoxSearchIndex {
	
	/**
	 * Test to insert and to read the bounding boxes
	 */
	@Test
	public void testBoxesInsert() {
		final List<Tuple> tupleList = getTupleList();
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
	}
	
	@Test
	public void testQueryOnEmptytree() {
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		final List<String> result = boxSortIndex.query(new BoundingBox(1d, 1d, 2d, 2d));
		Assert.assertTrue(result.isEmpty());
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery1() {
		final List<Tuple> tupleList = getTupleList();
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
		queryIndex(tupleList, boxSortIndex);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery2() {
		final List<Tuple> tupleList = generateRandomTupleList(2);
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
		queryIndex(tupleList, boxSortIndex);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery3() {
		final List<Tuple> tupleList = generateRandomTupleList(3);
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
		
		queryIndex(tupleList, boxSortIndex);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery4() {
		final List<Tuple> tupleList = generateRandomTupleList(4);
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
		
		queryIndex(tupleList, boxSortIndex);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery5() {
		final List<Tuple> tupleList = generateRandomTupleList(5);
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
		
		queryIndex(tupleList, boxSortIndex);
	}
	
	/**
	 * Test to query the index
	 */
	@Test
	public void testBoxQuery6() {
		final List<Tuple> tupleList = generateRandomTupleList(10);
		
		final BoxSortSpatialIndexStrategy boxSortIndex = new BoxSortSpatialIndexStrategy();
		boxSortIndex.constructFromTuples(tupleList);
		
		queryIndex(tupleList, boxSortIndex);
	}

	/**
	 * Test the query
	 * 
	 * @param tupleList
	 * @param boxSortIndex
	 */
	protected void queryIndex(final List<Tuple> tupleList, final BoxSortSpatialIndexStrategy boxSortIndex) {
		
		for(final Tuple tuple: tupleList) {
			final List<String> resultList = boxSortIndex.query(tuple.getBoundingBox());
			Assert.assertTrue(resultList.size() >= 1);
			Assert.assertTrue(resultList.contains(tuple.getKey()));
		}
	}

	/**
	 * Generate a list of tuples
	 * @return
	 */
	protected List<Tuple> getTupleList() {
		final List<Tuple> tupleList = new ArrayList<Tuple>();
		tupleList.add(new Tuple("abc", new BoundingBox(0d, 1d, 0d, 1d), "abc".getBytes()));
		tupleList.add(new Tuple("def", new BoundingBox(1d, 2d, 1d, 3d), "abc".getBytes()));
		tupleList.add(new Tuple("fgh", new BoundingBox(2d, 3d, 0d, 1d), "abc".getBytes()));
		tupleList.add(new Tuple("ijk", new BoundingBox(3d, 4d, 3d, 7d), "abc".getBytes()));
		tupleList.add(new Tuple("lmn", new BoundingBox(1.2d, 2.2d, 0d, 1d), "abc".getBytes()));
		tupleList.add(new Tuple("ijk", new BoundingBox(4.6d, 5.6d, 0d, 1d), "abc".getBytes()));
		tupleList.add(new Tuple("dwe", new BoundingBox(5.2d, 6.2d, 4d, 5d), "abc".getBytes()));
		tupleList.add(new Tuple("gwd", new BoundingBox(5.1d, 6.1d, 0d, 1d), "abc".getBytes()));
		tupleList.add(new Tuple("fs3", new BoundingBox(6.1d, 7.1d, 0d, 1d), "abc".getBytes()));
		tupleList.add(new Tuple("xyz", new BoundingBox(8.1d, 9.1d, 2d, 5d), "abc".getBytes()));
		return tupleList;
	}
	
	/**
	 * Generate some random tuples
	 * @return
	 */
	protected List<Tuple> generateRandomTupleList(int dimensions) {
		final List<Tuple> tupleList = new ArrayList<Tuple>();
		final Random random = new Random();
		
		for(int i = 0; i < 5000; i++) {
			final double[] boundingBoxData = new double[dimensions * 2];
			
			for(int d = 0; d < dimensions; d++) {
				final double begin = random.nextInt() % 1000;
				final double extent = Math.abs(random.nextInt() % 1000);
				boundingBoxData[2 * d] = begin;            // Start coordinate
				boundingBoxData[2 * d + 1] = begin+extent; // End coordinate
			}
			
			final Tuple tuple = new Tuple(Integer.toBinaryString(i), new BoundingBox(boundingBoxData), Integer.toBinaryString(i).getBytes());
			tupleList.add(tuple);
		}

		return tupleList;
	}

}

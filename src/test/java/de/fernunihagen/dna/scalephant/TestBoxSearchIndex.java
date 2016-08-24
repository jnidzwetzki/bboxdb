package de.fernunihagen.dna.scalephant;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.spatialindex.BoxSortSpatialIndexStrategy;

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
		final List<String> result = boxSortIndex.query(new BoundingBox(1f, 1f, 2f, 2f));
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
		tupleList.add(new Tuple("abc", new BoundingBox(0f, 1f, 0f, 1f), "abc".getBytes()));
		tupleList.add(new Tuple("def", new BoundingBox(1f, 2f, 1f, 3f), "abc".getBytes()));
		tupleList.add(new Tuple("fgh", new BoundingBox(2f, 3f, 0f, 1f), "abc".getBytes()));
		tupleList.add(new Tuple("ijk", new BoundingBox(3f, 4f, 3f, 7f), "abc".getBytes()));
		tupleList.add(new Tuple("lmn", new BoundingBox(1.2f, 2.2f, 0f, 1f), "abc".getBytes()));
		tupleList.add(new Tuple("ijk", new BoundingBox(4.6f, 5.6f, 0f, 1f), "abc".getBytes()));
		tupleList.add(new Tuple("dwe", new BoundingBox(5.2f, 6.2f, 4f, 5f), "abc".getBytes()));
		tupleList.add(new Tuple("gwd", new BoundingBox(5.1f, 6.1f, 0f, 1f), "abc".getBytes()));
		tupleList.add(new Tuple("fs3", new BoundingBox(6.1f, 7.1f, 0f, 1f), "abc".getBytes()));
		tupleList.add(new Tuple("xyz", new BoundingBox(8.1f, 9.1f, 2f, 5f), "abc".getBytes()));
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
			final float[] boundingBoxData = new float[dimensions * 2];
			
			for(int d = 0; d < dimensions; d++) {
				final float begin = random.nextInt() % 1000;
				final float extent = Math.abs(random.nextInt() % 1000);
				boundingBoxData[2 * d] = begin;            // Start coordinate
				boundingBoxData[2 * d + 1] = begin+extent; // End coordinate
			}
			
			final Tuple tuple = new Tuple(Integer.toBinaryString(i), new BoundingBox(boundingBoxData), Integer.toBinaryString(i).getBytes());
			tupleList.add(tuple);
		}

		return tupleList;
	}

}

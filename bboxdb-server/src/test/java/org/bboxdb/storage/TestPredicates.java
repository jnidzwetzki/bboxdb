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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.predicate.AndPredicate;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsVersionTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.OverlapsBoundingBoxPredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateTupleFilterIterator;
import org.junit.Assert;
import org.junit.Test;

public class TestPredicates {

	/**
	 * Test the newer as predicate
	 * @throws Exception
	 */
	@Test
	public void testNewerAsPredicate1() throws Exception {
		final Tuple tuple1 = new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tuple2 = new Tuple("2", BoundingBox.FULL_SPACE, "def".getBytes(), 2);
		
		final List<Tuple> tupleList = new ArrayList<>();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		
		final Predicate predicate = new NewerAsVersionTimePredicate(0);
		final Collection<Tuple> tuples = getTuplesFromPredicate(tupleList, predicate);
		
		System.out.println(tuples);
		
		Assert.assertEquals(2, tuples.size());
		Assert.assertTrue(tuples.contains(tuple1));
		Assert.assertTrue(tuples.contains(tuple2));
	}
	
	/**
	 * Test the newer as predicate
	 * @throws Exception
	 */
	@Test
	public void testNewerAsPredicate2() throws Exception {
		final Tuple tuple1 = new Tuple("1", BoundingBox.FULL_SPACE, "abc".getBytes(), 50);
		final Tuple tuple2 = new Tuple("2", BoundingBox.FULL_SPACE, "def".getBytes(), 1234);

		final List<Tuple> tupleList = new ArrayList<>();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		
		final Predicate predicate = new NewerAsVersionTimePredicate(51);
		final Collection<Tuple> tuples = getTuplesFromPredicate(tupleList, predicate);
		
		Assert.assertEquals(1, tuples.size());
		Assert.assertFalse(tuples.contains(tuple1));
		Assert.assertTrue(tuples.contains(tuple2));
	}
	
	/**
	 * Test the bounding box predicate
	 * @throws Exception
	 */
	@Test
	public void boundingBoxPredicate() {
		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 10.0, 1.0, 10.9), "abc".getBytes(), 50);
		final Tuple tuple2 = new Tuple("2", new BoundingBox(-11.0, 0.0, -11.0, 0.9), "def".getBytes(), 1234);

		final List<Tuple> tupleList = new ArrayList<>();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		
		final Predicate predicate1 = new OverlapsBoundingBoxPredicate(new BoundingBox(-100.0, 100.0, -100.0, 100.0));
		final Collection<Tuple> tuples1 = getTuplesFromPredicate(tupleList, predicate1);
		
		Assert.assertEquals(2, tuples1.size());
		Assert.assertTrue(tuples1.contains(tuple1));
		Assert.assertTrue(tuples1.contains(tuple2));
		
		final Predicate predicate2 = new OverlapsBoundingBoxPredicate(new BoundingBox(2.0, 100.0, 2.0, 100.0));
		final Collection<Tuple> tuples2 = getTuplesFromPredicate(tupleList, predicate2);
		
		Assert.assertEquals(1, tuples2.size());
		Assert.assertTrue(tuples2.contains(tuple1));
		Assert.assertFalse(tuples2.contains(tuple2));
	}
	
	/**
	 * Test the and predicate
	 * @throws Exception
	 */
	@Test
	public void boundingAndPredicate() {
		final Tuple tuple1 = new Tuple("1", new BoundingBox(1.0, 10.0, 1.0, 10.9), "abc".getBytes(), 50);
		final Tuple tuple2 = new Tuple("2", new BoundingBox(-11.0, 0.0, -11.0, 0.9), "def".getBytes(), 1234);

		final List<Tuple> tupleList = new ArrayList<>();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		
		final Predicate predicate1 = new OverlapsBoundingBoxPredicate(new BoundingBox(2.0, 100.0, 2.0, 100.0));
		final Predicate predicate2 = new NewerAsVersionTimePredicate(51);
		
		final Predicate predicate = new AndPredicate(predicate1, predicate2);
		
		final Collection<Tuple> tuples = getTuplesFromPredicate(tupleList, predicate);

		Assert.assertTrue(tuples.isEmpty());
	}
		
	/**
	 * Get all tuples that matches the given predicate
	 * @param tupleList
	 * @param predicate
	 * @return
	 */
	protected Collection<Tuple> getTuplesFromPredicate(final List<Tuple> tupleList, 
			final Predicate predicate) {
		
		final Iterator<Tuple> iterator = new PredicateTupleFilterIterator(tupleList.iterator(), predicate);

		final Collection<Tuple> result = new ArrayList<>();
		
		while(iterator.hasNext()) {
			result.add(iterator.next());
		}
		
		return result;
	}
}

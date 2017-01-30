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
package org.bboxdb.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateFilterIterator;
import org.junit.Assert;
import org.junit.Test;

public class TestPredicates {

	@Test
	public void testNewerAsPredicate1() throws Exception {
		final Tuple tuple1 = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		final Tuple tuple2 = new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes(), 2);
		
		final List<Tuple> tupleList = new ArrayList<>();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		
		final NewerAsTimePredicate predicate = new NewerAsTimePredicate(0);
		final Collection<Tuple> tuples = getTuplesFromPredicate(tupleList, predicate);
		
		System.out.println(tuples);
		
		Assert.assertEquals(2, tuples.size());
		Assert.assertTrue(tuples.contains(tuple1));
		Assert.assertTrue(tuples.contains(tuple2));
	}
	
	@Test
	public void testNewerAsPredicate2() throws Exception {
		final Tuple tuple1 = new Tuple("1", BoundingBox.EMPTY_BOX, "abc".getBytes(), 50);
		final Tuple tuple2 = new Tuple("2", BoundingBox.EMPTY_BOX, "def".getBytes(), 1234);

		final List<Tuple> tupleList = new ArrayList<>();
		tupleList.add(tuple1);
		tupleList.add(tuple2);
		
		final NewerAsTimePredicate predicate = new NewerAsTimePredicate(51);
		final Collection<Tuple> tuples = getTuplesFromPredicate(tupleList, predicate);
		
		Assert.assertEquals(1, tuples.size());
		Assert.assertFalse(tuples.contains(tuple1));
		Assert.assertTrue(tuples.contains(tuple2));
	}
	
	protected Collection<Tuple> getTuplesFromPredicate(final List<Tuple> tupleList, 
			final Predicate predicate) {
		
		final Iterator<Tuple> iterator = new PredicateFilterIterator(tupleList.iterator(), predicate);

		final Collection<Tuple> result = new ArrayList<>();
		
		while(iterator.hasNext()) {
			result.add(iterator.next());
		}
		
		return result;
	}
}

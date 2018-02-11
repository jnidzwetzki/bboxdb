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

import java.util.Arrays;
import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.EntityDuplicateTracker;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleDuplicateRemover {

	@Test
	public void testTupleDuplicateRemoverDiffKey() {
		final EntityDuplicateTracker tupleDuplicateRemover = new EntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key2", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
	}
	
	@Test
	public void testTupleDuplicateRemoverEqualKeyDiffVesion() {
		final EntityDuplicateTracker tupleDuplicateRemover = new EntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 2);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
	}
	
	@Test
	public void testTupleDuplicateRemoverEqualKeyEqualVesion() {
		final EntityDuplicateTracker tupleDuplicateRemover = new EntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
	}
	
	@Test
	public void testJoinedTuple() {
	final EntityDuplicateTracker tupleDuplicateRemover = new EntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		final Tuple tuple2 = new Tuple("key2", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		final Tuple tuple3 = new Tuple("key2", BoundingBox.EMPTY_BOX, "".getBytes(), 1);

		final List<Tuple> tupleList1 = Arrays.asList(tuple1);
		final List<Tuple> tupleList2 = Arrays.asList(tuple1, tuple2);
		final List<Tuple> tupleList3 = Arrays.asList(tuple1, tuple2, tuple3);

		final JoinedTuple joinedTuple1 = new JoinedTuple(tupleList1, Arrays.asList("table1"));
		final JoinedTuple joinedTuple2 = new JoinedTuple(tupleList2, Arrays.asList("table1", "table2"));
		final JoinedTuple joinedTuple3 = new JoinedTuple(tupleList3, Arrays.asList("table1", "table2", "table3"));

		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple1));

		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple2));

		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple3));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple3));
	}

}

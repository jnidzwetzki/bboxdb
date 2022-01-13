/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.test.storage;

import java.util.Arrays;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TimeBasedEntityDuplicateTracker;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleDuplicateRemover {

	@Test(timeout=60000)
	public void testTupleDuplicateRemoverDiffKey() {
		final TimeBasedEntityDuplicateTracker tupleDuplicateRemover = new TimeBasedEntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key2", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
	}
	
	@Test(timeout=60000)
	public void testTupleDuplicateRemoverEqualKeyDiffVesion() {
		final TimeBasedEntityDuplicateTracker tupleDuplicateRemover = new TimeBasedEntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key1", Hyperrectangle.FULL_SPACE, "".getBytes(), 2);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
	}
	
	@Test(timeout=60000)
	public void testTupleDuplicateRemoverEqualKeyEqualVesion() {
		final TimeBasedEntityDuplicateTracker tupleDuplicateRemover = new TimeBasedEntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key1", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(tuple2));
	}
	
	@Test(timeout=60000)
	public void testJoinedTuple() {
	final TimeBasedEntityDuplicateTracker tupleDuplicateRemover = new TimeBasedEntityDuplicateTracker();
		
		final Tuple tuple1 = new Tuple("key1", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		final Tuple tuple2 = new Tuple("key2", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);
		final Tuple tuple3 = new Tuple("key2", Hyperrectangle.FULL_SPACE, "".getBytes(), 1);

		final List<Tuple> tupleList1 = Arrays.asList(tuple1);
		final List<Tuple> tupleList2 = Arrays.asList(tuple1, tuple2);
		final List<Tuple> tupleList3 = Arrays.asList(tuple1, tuple2, tuple3);

		final MultiTuple joinedTuple1 = new MultiTuple(tupleList1, Arrays.asList("table1"));
		final MultiTuple joinedTuple2 = new MultiTuple(tupleList2, Arrays.asList("table1", "table2"));
		final MultiTuple joinedTuple3 = new MultiTuple(tupleList3, Arrays.asList("table1", "table2", "table3"));

		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple1));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple1));

		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple2));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple2));

		Assert.assertFalse(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple3));
		Assert.assertTrue(tupleDuplicateRemover.isElementAlreadySeen(joinedTuple3));
	}

}

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

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleDuplicateRemover;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleDuplicateRemover {

	@Test
	public void testTupleDuplicateRemoverDiffKey() {
		final TupleDuplicateRemover tupleDuplicateRemover = new TupleDuplicateRemover();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isTupleAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key2", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isTupleAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple2));
	}
	
	@Test
	public void testTupleDuplicateRemoverEqualKeyDiffVesion() {
		final TupleDuplicateRemover tupleDuplicateRemover = new TupleDuplicateRemover();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isTupleAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 2);
		Assert.assertFalse(tupleDuplicateRemover.isTupleAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple2));
	}
	
	@Test
	public void testTupleDuplicateRemoverEqualKeyEqualVesion() {
		final TupleDuplicateRemover tupleDuplicateRemover = new TupleDuplicateRemover();
		
		final Tuple tuple1 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertFalse(tupleDuplicateRemover.isTupleAlreadySeen(tuple1));
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple1));

		final Tuple tuple2 = new Tuple("key1", BoundingBox.EMPTY_BOX, "".getBytes(), 1);
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple2));
		Assert.assertTrue(tupleDuplicateRemover.isTupleAlreadySeen(tuple2));
	}

}

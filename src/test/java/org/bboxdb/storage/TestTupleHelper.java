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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.TupleHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleHelper {

	/**
	 * Find the most recent tuple
	 */
	@Test
	public void testGetMostRecentTuple() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 2);
		
		Assert.assertEquals(null, TupleHelper.returnMostRecentTuple(null, null));
		Assert.assertEquals(tupleA, TupleHelper.returnMostRecentTuple(tupleA, null));
		Assert.assertEquals(tupleA, TupleHelper.returnMostRecentTuple(null, tupleA));

		Assert.assertEquals(tupleB, TupleHelper.returnMostRecentTuple(tupleA, tupleB));
		Assert.assertEquals(tupleB, TupleHelper.returnMostRecentTuple(tupleB, tupleA));
		Assert.assertEquals(tupleB, TupleHelper.returnMostRecentTuple(tupleB, tupleB));
	}
	
	/**
	 * Test the tuple resolver
	 */
	@Test
	public void testTupleDuplicateResolver() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		TupleHelper.NEWEST_TUPLE_DUPLICATE_RESOLVER.handleDuplicates(tupleList);
		Assert.assertEquals(1, tupleList.size());
		Assert.assertTrue(tupleList.contains(tupleD));
	}
	
	/**
	 * Test the tuple key comparator
	 */
	@Test
	public void testTupleKeyComparator() {
		final Tuple tupleA = new Tuple("xyz", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("ijk", BoundingBox.EMPTY_BOX, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.EMPTY_BOX, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC));
		
		tupleList.sort(TupleHelper.TUPLE_KEY_COMPARATOR);
		
		Assert.assertEquals(tupleC, tupleList.get(0));
		Assert.assertEquals(tupleB, tupleList.get(1));
		Assert.assertEquals(tupleA, tupleList.get(2));
	}
	
	/**
	 * Encode and decode a tuple
	 * @throws IOException 
	 */
	@Test
	public void encodeAndDecodeTuple1() throws IOException {
		final Tuple tuple = new Tuple("abc", new BoundingBox(1.0, 2.0, 3.0, 4.0), "abc".getBytes());
		
		// Read from stream
		final byte[] bytes = TupleHelper.tupleToBytes(tuple);
		final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		final Tuple readTuple = TupleHelper.decodeTuple(inputStream);
		Assert.assertEquals(tuple, readTuple);
		
		// Read from byte buffer
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final Tuple readTuple2 = TupleHelper.decodeTuple(bb);
		Assert.assertEquals(tuple, readTuple2);
	}
	
	
	/**
	 * Encode and decode a tuple
	 * @throws IOException 
	 */
	@Test
	public void encodeAndDecodeTuple2() throws IOException {
		final Tuple tuple = new DeletedTuple("abc");
		
		// Read from stream
		final byte[] bytes = TupleHelper.tupleToBytes(tuple);
		final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		final Tuple readTuple = TupleHelper.decodeTuple(inputStream);
		Assert.assertEquals(tuple, readTuple);
		
		// Read from byte buffer
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final Tuple readTuple2 = TupleHelper.decodeTuple(bb);
		Assert.assertEquals(tuple, readTuple2);
	}

	
	
}

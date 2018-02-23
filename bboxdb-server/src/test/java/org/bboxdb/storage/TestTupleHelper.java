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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.bboxdb.storage.sstable.duplicateresolver.NewestTupleDuplicateResolver;
import org.bboxdb.storage.sstable.duplicateresolver.TTLAndVersionTupleDuplicateResolver;
import org.bboxdb.storage.sstable.duplicateresolver.TTLTupleDuplicateResolver;
import org.bboxdb.storage.sstable.duplicateresolver.VersionTupleDuplicateResolver;
import org.bboxdb.storage.util.TupleHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleHelper {

	/**
	 * Find the most recent tuple
	 */
	@Test
	public void testGetMostRecentTuple() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		
		Assert.assertEquals(null, TupleHelper.returnMostRecentTuple(null, null));
		Assert.assertEquals(tupleA, TupleHelper.returnMostRecentTuple(tupleA, null));
		Assert.assertEquals(tupleA, TupleHelper.returnMostRecentTuple(null, tupleA));

		Assert.assertEquals(tupleB, TupleHelper.returnMostRecentTuple(tupleA, tupleB));
		Assert.assertEquals(tupleB, TupleHelper.returnMostRecentTuple(tupleB, tupleA));
		Assert.assertEquals(tupleB, TupleHelper.returnMostRecentTuple(tupleB, tupleB));
	}
	
	/**
	 * Test the duplicate tuple resolver
	 */
	@Test
	public void testTupleDuplicateResolverNewest() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		final DuplicateResolver<Tuple> resolver = new NewestTupleDuplicateResolver();
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(1, tupleList.size());
	}
	
	/**
	 * Test the tuple resolver
	 */
	@Test
	public void testTupleDuplicateResolverTTLAndVersion1() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		// basetime = 6, ttl = 3 / tuple older than 2 are removed with 10 duplicates
		final DuplicateResolver<Tuple> resolver = new TTLAndVersionTupleDuplicateResolver(4, 
				TimeUnit.MICROSECONDS, 10, 6);
		
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(3, tupleList.size());
		Assert.assertTrue(tupleList.contains(tupleD));
		Assert.assertTrue(tupleList.contains(tupleE));
		Assert.assertTrue(tupleList.contains(tupleB));
	}
	
	/**
	 * Test the tuple resolver
	 */
	@Test
	public void testTupleDuplicateResolverTTLAndVersion2() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		
		// basetime = 6, ttl = 3 / tuple older than 2 are removed with 10 duplicates
		final DuplicateResolver<Tuple> resolver = new TTLAndVersionTupleDuplicateResolver(4, 
				TimeUnit.MICROSECONDS, 1, 6);
		
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(1, tupleList.size());
		Assert.assertTrue(tupleList.contains(tupleD));
	}
	

	/**
	 * Test the tuple resolver
	 */
	@Test
	public void testTupleDuplicateResolverTTL() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		// basetime = 6, ttl = 3 / tuple older than 3 are removed
		final DuplicateResolver<Tuple> resolver = new TTLTupleDuplicateResolver(3, TimeUnit.MICROSECONDS, 6);
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(2, tupleList.size());
		Assert.assertTrue(tupleList.contains(tupleD));
		Assert.assertTrue(tupleList.contains(tupleE));
	}
	
	/**
	 * Test the tuple resolver - versions
	 */
	@Test
	public void testTupleDuplicateResolverVersions() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		final DuplicateResolver<Tuple> resolver = new VersionTupleDuplicateResolver(3);
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(3, tupleList.size());
		Assert.assertTrue(tupleList.contains(tupleB));
		Assert.assertTrue(tupleList.contains(tupleD));
		Assert.assertTrue(tupleList.contains(tupleE));
	}
	
	/**
	 * Test the do nothing tuple resolver
	 */
	@Test
	public void testDoNothingTupleDuplicateResolverVersions() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		final DuplicateResolver<Tuple> resolver = new DoNothingDuplicateResolver();
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(6, tupleList.size());
		Assert.assertTrue(tupleList.contains(tupleA));
		Assert.assertTrue(tupleList.contains(tupleB));
		Assert.assertTrue(tupleList.contains(tupleC));
		Assert.assertTrue(tupleList.contains(tupleD));
		Assert.assertTrue(tupleList.contains(tupleF));
	}
	
	
	/**
	 * Test the tuple key comparator
	 */
	@Test
	public void testTupleKeyComparator1() {
		final Tuple tupleA = new Tuple("xyz", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("ijk", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC));
		
		tupleList.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		Assert.assertEquals(tupleC, tupleList.get(0));
		Assert.assertEquals(tupleB, tupleList.get(1));
		Assert.assertEquals(tupleA, tupleList.get(2));
	}
	
	/**
	 * Test the tuple key comparator
	 */
	@Test
	public void testTupleKeyComparator2() {
		final Tuple tupleA = new Tuple("xyz", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("ijk", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 5);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);

		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, tupleD));
		
		tupleList.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		Assert.assertEquals(tupleD, tupleList.get(0));
		Assert.assertEquals(tupleC, tupleList.get(1));
		Assert.assertEquals(tupleB, tupleList.get(2));
		Assert.assertEquals(tupleA, tupleList.get(3));
	}
	
	/**
	 * Test the tuple key comparator
	 */
	@Test
	public void testTupleKeyComparator3() {
		final Tuple tupleA = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleB = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleC = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 5);
		final Tuple tupleD = new Tuple("abc", BoundingBox.FULL_SPACE, "abc".getBytes(), 2);

		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, tupleD));
		
		tupleList.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		Assert.assertEquals(tupleB, tupleList.get(0));
		Assert.assertEquals(tupleD, tupleList.get(1));
		Assert.assertEquals(tupleA, tupleList.get(2));
		Assert.assertEquals(tupleC, tupleList.get(3));
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

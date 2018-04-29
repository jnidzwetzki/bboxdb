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
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.JoinedTupleIdentifier;
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
	@Test(timeout=60000)
	public void testGetMostRecentTuple() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		
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
	@Test(timeout=60000)
	public void testTupleDuplicateResolverNewest() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC, 
				tupleD, tupleE, tupleF));
		
		final DuplicateResolver<Tuple> resolver = new NewestTupleDuplicateResolver();
		resolver.removeDuplicates(tupleList);
		
		Assert.assertEquals(1, tupleList.size());
	}
	
	/**
	 * Test the tuple resolver
	 */
	@Test(timeout=60000)
	public void testTupleDuplicateResolverTTLAndVersion1() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
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
	@Test(timeout=60000)
	public void testTupleDuplicateResolverTTLAndVersion2() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
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
	@Test(timeout=60000)
	public void testTupleDuplicateResolverTTL() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
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
	@Test(timeout=60000)
	public void testTupleDuplicateResolverVersions() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
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
	@Test(timeout=60000)
	public void testDoNothingTupleDuplicateResolverVersions() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleE = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 3);
		final Tuple tupleF = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
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
	@Test(timeout=60000)
	public void testTupleKeyComparator1() {
		final Tuple tupleA = new Tuple("xyz", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("ijk", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		
		final List<Tuple> tupleList = new ArrayList<>(Arrays.asList(tupleA, tupleB, tupleC));
		
		tupleList.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		Assert.assertEquals(tupleC, tupleList.get(0));
		Assert.assertEquals(tupleB, tupleList.get(1));
		Assert.assertEquals(tupleA, tupleList.get(2));
	}
	
	/**
	 * Test the tuple key comparator
	 */
	@Test(timeout=60000)
	public void testTupleKeyComparator2() {
		final Tuple tupleA = new Tuple("xyz", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleB = new Tuple("ijk", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 5);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);

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
	@Test(timeout=60000)
	public void testTupleKeyComparator3() {
		final Tuple tupleA = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 4);
		final Tuple tupleB = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 1);
		final Tuple tupleC = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 5);
		final Tuple tupleD = new Tuple("abc", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 2);

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
	@Test(timeout=60000)
	public void encodeAndDecodeTuple1() throws IOException {
		final Tuple tuple = new Tuple("abc", new Hyperrectangle(1.0, 2.0, 3.0, 4.0), "abc".getBytes());
		
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
	@Test(timeout=60000)
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

	/**
	 * Test misc methods of a tuple
	 */
	@Test(timeout=60000)
	public void testTupleMisc() {
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1d, 2d), "".getBytes());
		Assert.assertTrue(tuple1.compareTo(tuple1) == 0);
		Assert.assertTrue(tuple1.getFormatedString().length() > 10);
		
		final Tuple tuple2 = new DeletedTuple("abc");
		Assert.assertTrue(tuple2.getFormatedString().length() > 10);
	}
	
	/**
	 * Test misc function of the joined tuple
	 */
	@Test(timeout=60000)
	public void testJoinedTupleMisc() {
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1d, 2d), "".getBytes());
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(1d, 2d), "".getBytes());
		final Tuple tuple3 = new Tuple("yjk", new Hyperrectangle(1d, 2d), "".getBytes());

		final JoinedTuple joinedTuple1 = new JoinedTuple(Arrays.asList(tuple1, tuple2), Arrays.asList("abc", "def"));
		final JoinedTuple joinedTuple2 = new JoinedTuple(Arrays.asList(tuple2, tuple3), Arrays.asList("abc", "def"));
		final JoinedTuple joinedTuple3 = new JoinedTuple(Arrays.asList(tuple2), Arrays.asList("abc"));

		Assert.assertEquals(joinedTuple1, joinedTuple1);
		Assert.assertEquals(joinedTuple1.hashCode(), joinedTuple1.hashCode());

		Assert.assertEquals(joinedTuple2, joinedTuple2);
		Assert.assertEquals(joinedTuple2.hashCode(), joinedTuple2.hashCode());

		Assert.assertTrue(joinedTuple1.compareTo(joinedTuple2) < 0);
		Assert.assertTrue(joinedTuple2.compareTo(joinedTuple1) > 0);
		Assert.assertTrue(joinedTuple1.compareTo(joinedTuple1) == 0);
		Assert.assertTrue(joinedTuple1.compareTo(joinedTuple3) < 0);

		Assert.assertTrue(joinedTuple1.getFormatedString().length() > 10);
		Assert.assertTrue(joinedTuple2.getFormatedString().length() > 10);
		Assert.assertTrue(joinedTuple3.getFormatedString().length() > 10);
	}
	
	/**
	 * Test the convert method of multiple tuples
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConvertMultiTuple() {
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1d, 2d), "".getBytes());
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(1d, 2d), "".getBytes());

		final JoinedTuple joinedTuple1 = new JoinedTuple(Arrays.asList(tuple1, tuple2), Arrays.asList("abc", "def"));
		joinedTuple1.convertToSingleTupleIfPossible();
	}
	
	/**
	 * Test the creation with different sizes
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreateWithDifferentSizes() {
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1d, 2d), "".getBytes());
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(1d, 2d), "".getBytes());

		new JoinedTuple(Arrays.asList(tuple1, tuple2), Arrays.asList("abc"));
	}
	
	/**
	 * Test the joined tuple identifier
	 */
	@Test(timeout=60000)
	public void testJoinedTupleIdentifier() {
		final Tuple tuple1 = new Tuple("abc", new Hyperrectangle(1d, 2d), "".getBytes());
		final Tuple tuple2 = new Tuple("def", new Hyperrectangle(1d, 2d), "".getBytes());

		final JoinedTupleIdentifier joinedTupleIdentifier = 
				new JoinedTupleIdentifier(Arrays.asList(tuple1, tuple2), Arrays.asList("abc", "def"));
		
		Assert.assertTrue(joinedTupleIdentifier.toString().length() > 10);
	}
}

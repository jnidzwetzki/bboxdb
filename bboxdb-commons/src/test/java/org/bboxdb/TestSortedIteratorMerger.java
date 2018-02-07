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
package org.bboxdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.SortedIteratorMerger;
import org.junit.Assert;
import org.junit.Test;

public class TestSortedIteratorMerger {
	
	/**
	 * Default String comparator
	 */
	protected final Comparator<String> STRING_COMPARATOR = (e1, e2) -> (e1.compareTo(e2));
	
	/**
	 * The return all duplicate resolver
	 */
	protected final DuplicateResolver<String> DEFAULT_DUPLICATE_RESOLVER = (e) -> {};
	
	/**
	 * The return the first element resolver
	 */
	protected final DuplicateResolver<String> FIRST_ELEMENT_DUPLICATE_RESOLVER = (e) -> {
		final String element = e.get(0);
		e.clear();
		e.add(element);
	};

	/**
	 * Test merger with one iterator
	 */
	@Test
	public void testMergeOneIterator1() {
		final String stringA = "a";
		
		final List<String> list1 = Arrays.asList(stringA);
		
		final List<Iterator<String>> iteratorList = Arrays.asList(list1.iterator());
		
		final SortedIteratorMerger<String> sortedIteratorMerger = new SortedIteratorMerger<>(iteratorList, STRING_COMPARATOR, DEFAULT_DUPLICATE_RESOLVER);

		final List<String> resultList = getResultList(sortedIteratorMerger);
		
		Assert.assertEquals(1, resultList.size());
	}

	/**
	 * Test merger with one iterator
	 */
	@Test
	public void testMergeOneIterator2() {
		final String stringA = "a";
		final String stringB = "b";
		final String stringC = "c";

		final List<String> list1 = Arrays.asList(stringA, stringB, stringC);
		
		final List<Iterator<String>> iteratorList = Arrays.asList(list1.iterator());
		
		final SortedIteratorMerger<String> sortedIteratorMerger = new SortedIteratorMerger<>(iteratorList, STRING_COMPARATOR, DEFAULT_DUPLICATE_RESOLVER);

		final List<String> resultList = getResultList(sortedIteratorMerger);
		
		Assert.assertEquals(3, resultList.size());
	}
	
	/**
	 * Test merger with one iterator
	 */
	@Test
	public void testMergeTwoIterator1() {
		final String stringA = "a";
		final String stringB = "b";
		final String stringC = "c";

		final List<String> list1 = Arrays.asList(stringA, stringB, stringC);
		
		final List<Iterator<String>> iteratorList = Arrays.asList(list1.iterator(), list1.iterator());
		final SortedIteratorMerger<String> sortedIteratorMerger = new SortedIteratorMerger<>(iteratorList, STRING_COMPARATOR, DEFAULT_DUPLICATE_RESOLVER);

		final List<String> resultList = getResultList(sortedIteratorMerger);
		
		Assert.assertEquals(6, resultList.size());
	}
	
	/**
	 * @param iteratorList
	 * @return 
	 */
	private List<String> getResultList(final SortedIteratorMerger<String> iterator) {	
		final List<String> resultList = new ArrayList<>();
		iterator.iterator().forEachRemaining(resultList::add);
		
		return resultList;
	}
	
	/**
	 * Test empty list
	 */
	@Test
	public void testEmpty1() {

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				new ArrayList<Iterator<String>>(), 
				STRING_COMPARATOR, 
				DEFAULT_DUPLICATE_RESOLVER);
		
		Assert.assertFalse(mergeIterator.iterator().hasNext());
		
		final SortedIteratorMerger<String> mergeIterator2 = new SortedIteratorMerger<String>(
				new ArrayList<Iterator<String>>(), 
				STRING_COMPARATOR, 
				FIRST_ELEMENT_DUPLICATE_RESOLVER);
		
		Assert.assertFalse(mergeIterator2.iterator().hasNext());
	}
	
	/**
	 * Test empty list
	 */
	@Test
	public void testEmpty2() {
		
		final List<String> list1 = new ArrayList<>();
		final List<String> list2 = new ArrayList<>();
		final List<String> list3 = new ArrayList<>();

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator()), 
				STRING_COMPARATOR, 
				DEFAULT_DUPLICATE_RESOLVER);
		
		Assert.assertFalse(mergeIterator.iterator().hasNext());
	}
	
	
	/**
	 * Test one list
	 */
	@Test
	public void testOne() {
		final List<String> list1 = Arrays.asList("abc", "def", "geh");
		final List<String> list2 = new ArrayList<>();
		final List<String> list3 = new ArrayList<>();

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator()), 
				STRING_COMPARATOR, 
				DEFAULT_DUPLICATE_RESOLVER);
		
		final List<String> resultList = getResultList(mergeIterator);
		Assert.assertEquals(list1, resultList);
	}
	
	/**
	 * Test two lists
	 */
	@Test
	public void testTwo() {
		final List<String> list1 = Arrays.asList("abc", "def", "geh");
		final List<String> list2 = Arrays.asList("ijk");
		final List<String> list3 = new ArrayList<>();

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator()), 
				STRING_COMPARATOR, 
				DEFAULT_DUPLICATE_RESOLVER);
		
		final List<String> resultList = getResultList(mergeIterator);
		Assert.assertEquals(4, resultList.size());
	}
	
	/**
	 * Test three lists
	 */
	@Test
	public void testThree() {
		final List<String> list1 = Arrays.asList("abc", "def", "geh");
		final List<String> list2 = Arrays.asList("ijk");
		final List<String> list3 = Arrays.asList("opq", "rst", null);

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator()), 
				STRING_COMPARATOR, 
				DEFAULT_DUPLICATE_RESOLVER);
		
		final List<String> resultList = getResultList(mergeIterator);
		Assert.assertEquals(6, resultList.size());
	}
	
	/**
	 * Test three lists
	 */
	@Test
	public void testThreeEqualList() {
		final List<String> list1 = Arrays.asList("abc", "def", "geh");

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list1.iterator(), list1.iterator()), 
				STRING_COMPARATOR, 
				DEFAULT_DUPLICATE_RESOLVER);
		
		final List<String> resultList = getResultList(mergeIterator);
		Assert.assertEquals(9, resultList.size());
		Assert.assertEquals(9, mergeIterator.getReadElements());
	}
	
	/**
	 * Test three lists
	 */
	@Test
	public void testThreeEqualListDuplicateResolver() {
		final List<String> list1 = Arrays.asList("abc", "def", "geh");

		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list1.iterator(), list1.iterator()), 
				STRING_COMPARATOR, 
				FIRST_ELEMENT_DUPLICATE_RESOLVER);
		
		final List<String> resultList = getResultList(mergeIterator);
		Assert.assertEquals(3, resultList.size());
		Assert.assertEquals(list1, resultList);
		Assert.assertEquals(9, mergeIterator.getReadElements());
	}
	
	/**
	 * Test the first element duplicate resolver
	 */
	@Test
	public void firstElementDuplicateResolver() {
		final List<String> list1 = Arrays.asList("abc", "def", "geh");
		final List<String> list2 = Arrays.asList("def", "geh");
		final List<String> list3 = Arrays.asList("abc", "def");
		
		final SortedIteratorMerger<String> mergeIterator = new SortedIteratorMerger<String>(
				Arrays.asList(list1.iterator(), list2.iterator(), list3.iterator()), 
				STRING_COMPARATOR, 
				FIRST_ELEMENT_DUPLICATE_RESOLVER);

		final List<String> resultList = getResultList(mergeIterator);
		Assert.assertEquals(3, resultList.size());
		Assert.assertTrue(resultList.contains("abc"));
		Assert.assertTrue(resultList.contains("def"));
		Assert.assertTrue(resultList.contains("geh"));
		Assert.assertEquals(7, mergeIterator.getReadElements());
	}
}

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.ListHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestListHelper {

	/**
	 * Try to get element from empty list
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testRandom1() {
		final List<String> list = new ArrayList<>();
		ListHelper.getElementRandom(list);
	}
	
	/**
	 * Test random get
	 */
	@Test(timeout=60000)
	public void testRandom2() {
		final List<String> list = Arrays.asList("a", "b", "c", "d", "e", "f");
		
		final Map<String, Integer> counterMap = new HashMap<>();

		for(int i = 0; i < 1000; i++) {
			final String element = ListHelper.getElementRandom(list);
			Assert.assertTrue(list.contains(element));
			final int oldCount = counterMap.getOrDefault(element, 0);
			counterMap.put(element, oldCount + 1);
		}
		
		Assert.assertEquals(list.size(), counterMap.size());
		Assert.assertEquals(0, counterMap.values().stream().filter(e -> e == 0).count());
	}
	
	/**
	 * Get all combinations
	 */
	@Test(timeout=60000)
	public void testCombinations1() {
		final List<String> list1 = new ArrayList<>();
		final List<String> list2 = new ArrayList<>();

		final List<List<String>> result = ListHelper.getCombinations(list1, list2);
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(0, result.get(0).size());
	}
	
	/**
	 * Get all combinations
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCombinations2() {
		final List<String> list1 = new ArrayList<>();
		final List<String> list2 = Arrays.asList("abc");

		ListHelper.getCombinations(list1, list2);
	}
	
	/**
	 * Get all combinations
	 */
	@Test(timeout=60000)
	public void testCombinations3() {
		final List<String> list1 = Arrays.asList("a");
		final List<String> list2 = Arrays.asList("1");

		final List<List<String>> result = ListHelper.getCombinations(list1, list2);
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(1, result.get(0).size());
		Assert.assertTrue(result.get(0).contains("a"));
		Assert.assertTrue(result.get(1).contains("1"));
	}
	
	/**
	 * Get all combinations
	 */
	@Test(timeout=60000)
	public void testCombinations4() {
		final List<String> list1 = Arrays.asList("a", "b");
		final List<String> list2 = Arrays.asList("1", "2");

		final List<List<String>> result = ListHelper.getCombinations(list1, list2);
		Assert.assertEquals(4, result.size());
		Assert.assertEquals(2, result.get(0).size());
	}
	
	/**
	 * Get all combinations
	 */
	@Test(timeout=60000)
	public void testCombinations5() {
		final List<String> list1 = Arrays.asList("a", "b", "c");
		final List<String> list2 = Arrays.asList("1", "2", "3");

		final List<List<String>> result = ListHelper.getCombinations(list1, list2);
		Assert.assertEquals(8, result.size());
		Assert.assertEquals(3, result.get(0).size());
		
		Assert.assertTrue(result.contains(Arrays.asList("a", "b", "c")));
		Assert.assertTrue(result.contains(Arrays.asList("a", "b", "3")));
		Assert.assertTrue(result.contains(Arrays.asList("a", "2", "c")));
		Assert.assertTrue(result.contains(Arrays.asList("a", "2", "3")));
		Assert.assertTrue(result.contains(Arrays.asList("1", "b", "c")));
		Assert.assertTrue(result.contains(Arrays.asList("1", "b", "3")));
		Assert.assertTrue(result.contains(Arrays.asList("1", "2", "c")));
		Assert.assertTrue(result.contains(Arrays.asList("1", "2", "3")));
	}
}

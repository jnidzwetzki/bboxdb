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
package org.bboxdb;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.commons.IsotoneStringMapper;
import org.junit.Assert;
import org.junit.Test;


public class TestIsotoneStringMapper {
	
	@Test(timeout = 60_000)
	public void testDoubleMapping() {
		
		final List<String> elements1 = Arrays.asList("Abc", "Def", "Hij");
		sortMapAndTest(elements1);
		
		final List<String> elements2 = Arrays.asList("Abc", "Abc", "Y", "A", "A", "AA");
		sortMapAndTest(elements2);
		
		final List<String> elements3 = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I");
		sortMapAndTest(elements3);
		
		final List<String> elements4 = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");
		sortMapAndTest(elements4);
		
		final List<String> elements5 = Arrays.asList("A", "AA", "AAA", "AAAA", "AAAAA", "AAAAAA", "AAAAAAA", "AAAAAAAA", "AAAAAAAAA");
		sortMapAndTest(elements5);
		
		final List<String> elements6 = Arrays.asList("Cat", "Dog", "Hippo", "Hippopotamus", "Elephant", "Mouse", "Fox");
		sortMapAndTest(elements6);
	}

	/** 
	 * Sort the given list, perform the mapping and test the ordering
	 * @param elements
	 */
	private void sortMapAndTest(final List<String> elements) {
		
		elements.sort(Comparator.comparing(String::toString)); 
		
		//System.out.println("Natural order:" + elements);
		
		// Map with different prefix lengths
		for(int prefix = 0; prefix <= IsotoneStringMapper.DEFAULT_PREFIX_LENGTH; prefix++) {
			
			final int curPrefix = prefix;
			
			final List<Integer> mappedElements = elements
				.stream()
				.map(s -> IsotoneStringMapper.mapToInt(s, curPrefix))
				.collect(Collectors.toList());
			
			//System.out.println("Result Mapping (" + prefix + "): " + mappedElements);
			
			// Test Elements
			for(int i = 1; i < mappedElements.size(); i++) {
				Assert.assertTrue(mappedElements.get(i - 1) <= mappedElements.get(i));
			}
		}
		
	}
	
	@Test(timeout=60_00)
	public void testIsotoneMapping() {
		Assert.assertEquals(658188, IsotoneStringMapper.mapToInt("abc", 3), 0.00001);
	}

}

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
package org.bboxdb.commons;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class ListHelper {

	/**
	 * Our random source
	 */
	private final static Random random = new Random();

	/**
	 * Get a random element from the list
	 * @param list
	 * @return
	 */
	public static <T> T getElementRandom(final List<T> list) {
		
		if(list.isEmpty()) {
			throw new IllegalArgumentException("Unable to get element from empty list");
		}
		
		final int pos = random.nextInt(list.size());
		return list.get(pos);
	}
	
	/**
	 * Get all combinations
	 * 
	 * List1: a, b, c
	 * List2: 1, 2, 3
	 * 
	 * Result: abc, ab3, a2c, a23, 1bc, 1b3, 12c, 123
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static <T> List<List<T>> getCombinations(final List<T> list1, final List<T> list2) {
		
		if(list1.size() != list2.size()) {
			throw new IllegalArgumentException("Lists do not have the same size");
		}
		
		final List<List<T>> result = new ArrayList<>();
		
		final long combinations = (long) Math.pow(2, list1.size());
				
		// Count up to all possible combinations and create a bit field
		for(long combination = 0; combination < combinations; combination++) {
			final BitSet bitSet = BitSet.valueOf(new long[] {combination});
		
			final List<T> oneResult = new ArrayList<>();
			
			// Choose input lists according to the bit field
			for(int pos = 0; pos < list1.size(); pos++) {
				if(bitSet.get(pos) == false) {
					oneResult.add(list1.get(pos));
				} else {
					oneResult.add(list2.get(pos));
				}
			}
			result.add(oneResult);
		}
		
		return result;
	}
}

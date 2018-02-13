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
}

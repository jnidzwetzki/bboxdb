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
package org.bboxdb.storage.queryprocessor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.storage.entity.Tuple;

import com.google.common.collect.Iterators;

public class IteratorHelper {

	/**
	 * Count the elements for predicate
	 * @param predicate
	 * @return
	 */
	public static int getIteratorSize(final Iterator<Tuple> iterator) {
		return Iterators.size(iterator);
	}
	
	/**
	 * Add all elements of an iterator to a result list
	 * @param iterator
	 * @return
	 */
	public static <T> List<T> iteratorToList(final Iterator<T> iterator) {
		final List<T> resultList = new ArrayList<T>();
		iterator.forEachRemaining(resultList::add);
		return resultList;
	}

}

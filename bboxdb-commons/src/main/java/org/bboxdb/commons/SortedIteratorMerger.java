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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class merges a set with sorted iterators and returns a sorted 
 * list of elements. Duplicates are eliminated according the 
 * duplicateResolver
 *
 * @param <E>
 */
public class SortedIteratorMerger<E> implements Iterable<E> {

	/**
	 * The iterator map, contains all iterators and the
	 * last fetched element
	 */
	protected final Map<Iterator<E>, E> iteratorElementMap;
	
	/**
	 * The element comparator
	 */
	protected Comparator<? super E> elementComparator;

	/**
	 * The duplicate resolver
	 */
	protected final DuplicateResolver<E> duplicateResolver;

	/**
	 * The amount of read elements
	 */
	protected int readElements = 0;

	public SortedIteratorMerger(final List<Iterator<E>> iteratorList, 
			final Comparator<? super E> elementComparator,
			final DuplicateResolver<E> duplicateResolver) {
		
		this.elementComparator = Objects.requireNonNull(elementComparator);
		this.duplicateResolver = Objects.requireNonNull(duplicateResolver);
		Objects.requireNonNull(iteratorList);

		iteratorElementMap = new HashMap<>();
		
		for(final Iterator<E> iterator : iteratorList) {
			iteratorElementMap.put(iterator, null);
			refreshIterator(iterator);
		}		
	}
	
	/**
	 * Refresh the specified iterator
	 * @param iterator
	 */
	protected E refreshIterator(final Iterator<E> iterator) {
		
		if(! iterator.hasNext()) {
			iteratorElementMap.put(iterator, null);
			return null;
		}
		
		final E element = iterator.next();
		readElements++;
		
		iteratorElementMap.put(iterator, element);
		
		return element;
	}

	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {
			
			/**
			 * The list with unconsumed duplicates for the current key
			 */
			final List<E> unconsumedDuplicates = new ArrayList<E>();

			@Override
			public boolean hasNext() {
				
				if(! unconsumedDuplicates.isEmpty()) {
					return true;
				}
				
				// Any new values left?
				return iteratorElementMap.values()
						.stream()
						.anyMatch(e -> Objects.nonNull(e));
			}

			@Override
			public E next() {
				
				assert (hasNext() == true);
				
				// Consume the duplicates first 
				if(! unconsumedDuplicates.isEmpty()) {
					return unconsumedDuplicates.remove(0);
				}
				
				final E lowestElement = iteratorElementMap
						.values()
						.stream()
						.filter(e -> Objects.nonNull(e))
						.min(elementComparator)
						.orElse(null);
				
				assert (lowestElement != null);
				
				for(final Iterator<E> iteratorToCheck : iteratorElementMap.keySet()) {
					E element = iteratorElementMap.get(iteratorToCheck);
					
					// Move the searched element from the iterator list to the result list
					while(belongsElementToCurrentKey(lowestElement, element)) {	
						unconsumedDuplicates.add(element);
						element = refreshIterator(iteratorToCheck);
					}
				}
				
				assert (! unconsumedDuplicates.isEmpty());
				duplicateResolver.removeDuplicates(unconsumedDuplicates);
				assert (! unconsumedDuplicates.isEmpty());
				
				
				return unconsumedDuplicates.remove(0);				
			}

			/**
			 * Belongs the given element to the current key
			 * @param lowestElement
			 * @param element
			 * @return
			 */
			protected boolean belongsElementToCurrentKey(final E lowestElement, final E element) {
				if(element == null) {
					return false;
				}
						
				if(elementComparator.compare(element, lowestElement) == 0) {
					return true;
				}
				
				return false;
			}
		};
	}

	/**
	 * Get the number of read elements
	 * @return
	 */
	public int getReadElements() {
		return readElements;
	}
}

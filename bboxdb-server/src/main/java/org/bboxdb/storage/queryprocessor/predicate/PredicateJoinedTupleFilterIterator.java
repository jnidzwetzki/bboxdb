/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.storage.queryprocessor.predicate;

import java.util.Iterator;

import org.bboxdb.storage.entity.MultiTuple;

public class PredicateJoinedTupleFilterIterator implements Iterator<MultiTuple> {

	/**
	 * The base iterator
	 */
	private final Iterator<MultiTuple> baseIterator;
	
	/**
	 * The filter predicate
	 */
	private final Predicate predicate;
	
	/**
	 * The next available tuple
	 */
	private MultiTuple nextTuple = null;
	
	public PredicateJoinedTupleFilterIterator(final Iterator<MultiTuple> baseIterator, final Predicate predicate) {
		this.baseIterator = baseIterator;
		this.predicate = predicate;
	}

	@Override
	public boolean hasNext() {
		
		if(nextTuple != null) {
			return true;
		}
		
		// Search for the next predicate matching tuple
		while(baseIterator.hasNext()) {
			final MultiTuple tuple = baseIterator.next();
			
			if(tuple.getNumberOfTuples() != 1) {
				throw new IllegalArgumentException("Unable to filter tuple: " + tuple);
			}
			
			if(predicate.matches(tuple.getTuple(0))) {
				nextTuple = tuple;
				return true;
			}
		}
		
		return false;
	}

	@Override
	public MultiTuple next() {

		if(nextTuple == null) {
			throw new IllegalArgumentException("Invalid state, did you really called hasNext()?");
		}
				
		final MultiTuple resultTuple = nextTuple;
		nextTuple = null;
		return resultTuple;
	}

}

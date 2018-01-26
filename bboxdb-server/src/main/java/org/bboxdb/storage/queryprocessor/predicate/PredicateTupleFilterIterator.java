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
package org.bboxdb.storage.queryprocessor.predicate;

import java.util.Iterator;

import org.bboxdb.storage.entity.Tuple;

public class PredicateTupleFilterIterator implements Iterator<Tuple> {

	/**
	 * The base iterator
	 */
	protected final Iterator<Tuple> baseIterator;
	
	/**
	 * The filter predicate
	 */
	protected final Predicate predicate;
	
	/**
	 * The next available tuple
	 */
	protected Tuple nextTuple = null;
	
	public PredicateTupleFilterIterator(final Iterator<Tuple> baseIterator, final Predicate predicate) {
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
			final Tuple tuple = baseIterator.next();
			
			if(predicate.matches(tuple)) {
				nextTuple = tuple;
				return true;
			}
		}
		
		return false;
	}

	@Override
	public Tuple next() {

		if(nextTuple == null) {
			throw new IllegalArgumentException("Invalid state, did you really called hasNext()?");
		}
		
		final Tuple resultTuple = nextTuple;
		nextTuple = null;
		return resultTuple;
	}

}

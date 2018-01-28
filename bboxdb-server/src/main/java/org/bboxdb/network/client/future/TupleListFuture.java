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
package org.bboxdb.network.client.future;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleListFuture extends AbstractListFuture<Tuple> {

	/**
	 * The duplicate resolver
	 */
	protected final DuplicateResolver<Tuple> duplicateResolver;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleListFuture.class);

	public TupleListFuture(final DuplicateResolver<Tuple> duplicateResolver) {
		super();
		this.duplicateResolver = duplicateResolver;
	}

	public TupleListFuture(final int numberOfFutures, final DuplicateResolver<Tuple> duplicateResolver) {
		super(numberOfFutures);
		this.duplicateResolver = duplicateResolver;
	}

	/**
	 * Return a iterator for all tuples
	 * @return
	 */
	@Override
	public Iterator<Tuple> iterator() {
		if(! isDone() ) {
			throw new IllegalStateException("Future is not done, unable to build iterator");
		}
		
		if( isFailed() ) {
			throw new IllegalStateException("The future has failed, unable to build iterator");
		}
		
		// Is at least result paged? So, we use the threaded iterator 
		// that requests more tuples/pages in the background
		final boolean pagedResult = resultComplete.values().stream().anyMatch(e -> e == false);
		
		if(pagedResult) {
			return new ThreadedTupleListFutureIterator(this);
		} else {
			return createSimpleIterator();
		}
		
	}

	/**
	 * Returns a simple iterator for non paged results
	 * @return
	 */
	protected Iterator<Tuple> createSimpleIterator() {
		final List<Tuple> allTuples = new ArrayList<>();
		
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			try {
				final List<Tuple> tupleResult = get(i);
				
				if(tupleResult != null) {
					for(final Tuple tuple : tupleResult) {
						allTuples.add(tuple);
					}
				}
				
			} catch (Exception e) {
				logger.error("Got exception while iterating", e);
			}
		}
		
		allTuples.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		// Remove duplicates
		duplicateResolver.removeDuplicates(allTuples);
		
		return allTuples.iterator();
	}
	
}

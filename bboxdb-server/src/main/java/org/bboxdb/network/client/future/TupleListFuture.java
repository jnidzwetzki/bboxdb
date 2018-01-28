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

import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;

public class TupleListFuture extends AbstractListFuture<Tuple> {

	/**
	 * The duplicate resolver
	 */
	protected final DuplicateResolver<Tuple> duplicateResolver;

	public TupleListFuture(final DuplicateResolver<Tuple> duplicateResolver) {
		super();
		this.duplicateResolver = duplicateResolver;
	}

	public TupleListFuture(final int numberOfFutures, final DuplicateResolver<Tuple> duplicateResolver) {
		super(numberOfFutures);
		this.duplicateResolver = duplicateResolver;
	}

	/**
	 * Create a new threaded iterator
	 * @return
	 */
	@Override
	protected ThreadedTupleListFutureIterator createThreadedIterator() {
		return new ThreadedTupleListFutureIterator(this);
	}
	
	/**
	 * Returns a simple iterator, used for non paged results
	 * @return
	 */
	@Override
	protected Iterator<Tuple> createSimpleIterator() {
		final List<Tuple> allTuples = getListWithAllResults();
		
		// Sort tuples
		allTuples.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);
		
		// Remove duplicates
		duplicateResolver.removeDuplicates(allTuples);
		
		return allTuples.iterator();
	}
	
}

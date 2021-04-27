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
package org.bboxdb.network.client.future.client;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.network.client.future.client.helper.ThreadedTupleListFutureIterator;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TimeBasedEntityDuplicateTracker;
import org.bboxdb.storage.util.TupleHelper;

public class TupleListFuture extends AbstractListFuture<Tuple> {

	/**
	 * The duplicate resolver
	 */
	private final DuplicateResolver<Tuple> duplicateResolver;
	
	/**
	 * The tablename for the read operation
	 */
	private final String tablename;

	public TupleListFuture(final Supplier<List<NetworkOperationFuture>> futures,
			final DuplicateResolver<Tuple> duplicateResolver, final String tablename) {

		super(futures);

		this.duplicateResolver = duplicateResolver;
		this.tablename = tablename;
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

		// Perform read repair
		final ReadRepair readRepair = new ReadRepair(tablename, futures);
		readRepair.performReadRepair(allTuples);

		final TimeBasedEntityDuplicateTracker entityDuplicateTracker = new TimeBasedEntityDuplicateTracker();

		final Iterator<Tuple> iterator = allTuples.iterator();
		while(iterator.hasNext()) {
			final Tuple nextElement = iterator.next();

			if(entityDuplicateTracker.isElementAlreadySeen(nextElement)) {
				iterator.remove();
			}
		}

		return allTuples.iterator();
	}
}

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
package org.bboxdb.storage.queryprocessor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.util.CloseableIterator;

public class SelectionQueryProcessor extends AbstractQueryProcessor {

	/**
	 * The query plan to evaluate
	 */
	protected final QueryPlan queryplan;
	
	public SelectionQueryProcessor(final QueryPlan queryplan, final TupleStoreManager ssTableManager) {
		super(ssTableManager);
		this.queryplan = queryplan;
	}
	
	public CloseableIterator<Tuple> iterator() {

		aquireStorage();
		
		return new CloseableIterator<Tuple>() {

			/**
			 * The active iterator
			 */
			protected Iterator<Tuple> activeIterator = null;
			
			/**
			 * The active storage
			 */
			protected ReadOnlyTupleStore activeStorage = null;
			
			/**
			 * The next precomputed tuple
			 */
			protected final List<Tuple> nextTuples = new ArrayList<>();
			
			/**
			 * Setup the next iterator
			 */
			protected void setupNewIterator() {
				activeIterator = null;
				activeStorage = null;

				// Find next iterator 
				while(! unprocessedStorages.isEmpty()) {
					activeStorage = unprocessedStorages.remove(0);
					activeIterator = queryplan.execute(activeStorage);
					
					if(activeIterator == null) {
						continue;
					}
					
					if(activeIterator.hasNext()) {
						return;
					}
				}
				
				activeIterator = null;
				activeStorage = null;
			}
			
			/**
			 * Fetch the next tuple from the iterator
			 * @throws StorageManagerException
			 */
			protected void setupNextTuple() throws StorageManagerException {
				if(ready == false) {
					throw new IllegalStateException("Iterator is not ready");
				}
				
				while(nextTuples.isEmpty()) {
					if(activeIterator == null || ! activeIterator.hasNext()) {
						setupNewIterator();
					}
					
					// All iterators are exhausted
					if(activeIterator == null) {
						return;
					}
		
					final Tuple possibleTuple = activeIterator.next();
				
					if(seenTuples.containsKey(possibleTuple.getKey())) {
						final long oldTimestamp = seenTuples.get(possibleTuple.getKey());
						
						if(oldTimestamp < possibleTuple.getVersionTimestamp()) {	
							logger.error("This sould not happen! Got newer tuple later in "
									+ "query processing. Unprocessed storages {}, aquired storage "
									+ "{}, new tuple {}, old timestamp", unprocessedStorages, 
									aquiredStorages, possibleTuple, oldTimestamp);
							
							seenTuples.put(possibleTuple.getKey(), possibleTuple.getVersionTimestamp());
						}
					} else {
						// Set nextTuple != null to exit the loop
						final List<Tuple> tupleVersions = getVersionsForTuple(possibleTuple, 
								activeStorage, queryplan);
						
						nextTuples.addAll(tupleVersions);
						seenTuples.put(possibleTuple.getKey(), possibleTuple.getVersionTimestamp());
					}
				}
			}
			
			@Override
			public boolean hasNext() {
		
				try {
					if(nextTuples.isEmpty()) {
						setupNextTuple();
					}
				} catch (StorageManagerException e) {
					logger.error("Got an exception while locating next tuple", e);
				}
				
				return (! nextTuples.isEmpty());
			}

			@Override
			public Tuple next() {

				if(ready == false) {
					throw new IllegalStateException("Iterator is not ready");
				}
				
				if(nextTuples.isEmpty()) {
					throw new IllegalStateException("Next tuple is empty, did you really call hasNext() before?");
				}
				
				return nextTuples.remove(0);
			}

			@Override
			public void close() throws Exception {
				cleanup();
			}
		};
	}
}

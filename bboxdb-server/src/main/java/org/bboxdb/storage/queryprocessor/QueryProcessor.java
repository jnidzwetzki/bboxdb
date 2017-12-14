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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryProcessor {

	/**
	 * The query plan to evaluate
	 */
	protected final QueryPlan queryplan;
	
	/**
	 * The sstable manager
	 */
	protected final TupleStoreManager tupleStoreManager;

	/**
	 * Is the iterator ready?
	 */
	protected boolean ready;
	
	/**
	 * The seen tuples<Key, Timestamp> map
	 */
	protected final Map<String, Long> seenTuples;
	
	/**
	 * The unprocessed storages
	 */
	protected final List<ReadOnlyTupleStore> unprocessedStorages;

	/**
	 * The aquired storages
	 */
	protected final List<ReadOnlyTupleStore> aquiredStorages;

	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
	
	
	public QueryProcessor(final QueryPlan queryplan, final TupleStoreManager ssTableManager) {
		this.queryplan = queryplan;
		this.tupleStoreManager = ssTableManager;
		this.ready = false;
		this.seenTuples = new HashMap<String, Long>();
		this.aquiredStorages = new LinkedList<ReadOnlyTupleStore>();
		this.unprocessedStorages = new LinkedList<ReadOnlyTupleStore>();
	}
	
	public CloseableIterator<Tuple> iterator() {

		prepareUnprocessedStorage();
		
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
							logger.warn("Unprocessded: {}", unprocessedStorages);
							logger.warn("Aquired: {}", aquiredStorages);
							logger.warn("Got newer tuple {} than {}", possibleTuple, oldTimestamp);
							seenTuples.put(possibleTuple.getKey(), possibleTuple.getVersionTimestamp());
						}
					} else {
						// Set nextTuple != null to exit the loop
						nextTuples.addAll(getVersionsForTuple(possibleTuple));
						seenTuples.put(possibleTuple.getKey(), possibleTuple.getVersionTimestamp());
					}
				}
			}
			
			/**
			 * Get the most recent version of the tuple
			 * e.g. Memtables can contain multiple versions of the key
		 	 * The iterator can return an outdated version
		 	 * 
			 * @param tuple
			 * @return
			 * @throws StorageManagerException 
			 */
			public List<Tuple> getVersionsForTuple(final Tuple tuple) throws StorageManagerException {
				
				final List<Tuple> resultTuples = new ArrayList<>();
				resultTuples.addAll(activeStorage.get(tuple.getKey()));
				
				for(final ReadOnlyTupleStore readOnlyTupleStorage : unprocessedStorages) {
					final List<Tuple> possibleTuples = readOnlyTupleStorage.get(tuple.getKey());
					resultTuples.addAll(possibleTuples);
				}
				
				final TupleStoreConfiguration tupleStoreConfiguration 
					= tupleStoreManager.getTupleStoreConfiguration();
				
				final DuplicateResolver<Tuple> resolver 
					= TupleDuplicateResolverFactory.build(tupleStoreConfiguration);
				
				// Removed unwanted tuples for key
				resolver.removeDuplicates(resultTuples);
				
				return resultTuples;
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
	
	/**
	 * Cleanup all aquired tables
	 */
	protected void cleanup() {
		ready = false;
		tupleStoreManager.releaseStorage(aquiredStorages);
		aquiredStorages.clear();
		unprocessedStorages.clear();
	}

	/**
	 * Prepare the unprocessed storage list
	 * @throws StorageManagerException 
	 */
	protected void prepareUnprocessedStorage()  {
		
		try {
			aquiredStorages.clear();
			aquiredStorages.addAll(tupleStoreManager.aquireStorage());
			
			unprocessedStorages.clear();
			unprocessedStorages.addAll(aquiredStorages);
	
			// Sort tables regarding the newest tuple timestamp 
			// The newest storage should be on top of the list
			unprocessedStorages.sort((storage1, storage2) 
					-> Long.compare(storage2.getNewestTupleVersionTimestamp(), 
							        storage1.getNewestTupleVersionTimestamp()));
			
			ready = true;
		
		} catch (StorageManagerException e) {
			logger.error("Unable to aquire tables", e);
			cleanup();
		}
	}
	
}

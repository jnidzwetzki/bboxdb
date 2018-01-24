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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectionQueryProcessor {

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
	protected static final Logger logger = LoggerFactory.getLogger(SelectionQueryProcessor.class);
	
	
	/**
	 * The query plan to evaluate
	 */
	protected final QueryPlan queryplan;
	
	public SelectionQueryProcessor(final QueryPlan queryplan, final TupleStoreManager tupleStoreManager) {
		this.tupleStoreManager = tupleStoreManager;
		this.ready = false;
		this.seenTuples = new HashMap<String, Long>();
		this.aquiredStorages = new LinkedList<ReadOnlyTupleStore>();
		this.unprocessedStorages = new LinkedList<ReadOnlyTupleStore>();
		this.queryplan = queryplan;
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
	protected void aquireStorage() {
		
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

	/**
	 * Get the most recent version of the tuple
	 * e.g. Memtables can contain multiple versions of the key
	 * The iterator can return an outdated version
	 * 
	 * @param tuple
	 * @return
	 * @throws StorageManagerException 
	 */
	protected List<Tuple> getVersionsForTuple(final Tuple tuple, final ReadOnlyTupleStore activeStorage, 
			final QueryPlan queryPlan) throws StorageManagerException {
		
		final List<Tuple> resultTuples = getAllTupleVersionsForKey(tuple.getKey(), activeStorage);
		
		final TupleStoreConfiguration tupleStoreConfiguration 
			= tupleStoreManager.getTupleStoreConfiguration();
		
		final DuplicateResolver<Tuple> resolver 
			= TupleDuplicateResolverFactory.build(tupleStoreConfiguration);
		
		// Removed unwanted tuples for key
		resolver.removeDuplicates(resultTuples);
		
		// Remove versions of the tuple that don't apply our query region
		resultTuples.removeIf(t -> ! t.getBoundingBox().overlaps(queryPlan.getSpatialRestriction()));
		
		return resultTuples;
	}

	/**
	 * Get all tuples for a given key
	 * @param tuple
	 * @param activeStorage
	 * @return
	 * @throws StorageManagerException
	 */
	protected List<Tuple> getAllTupleVersionsForKey(final String key, 
			final ReadOnlyTupleStore activeStorage) throws StorageManagerException {
		
		final List<Tuple> resultTuples = new ArrayList<>();
		resultTuples.addAll(activeStorage.get(key));
		
		for(final ReadOnlyTupleStore readOnlyTupleStorage : unprocessedStorages) {
			final List<Tuple> possibleTuples = readOnlyTupleStorage.get(key);
			resultTuples.addAll(possibleTuples);
		}
		
		return resultTuples;
	}
	
	/**
	 * Get the tuple store name
	 * @return
	 */
	public TupleStoreName getTupleStoreName() {
		return tupleStoreManager.getTupleStoreName();
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

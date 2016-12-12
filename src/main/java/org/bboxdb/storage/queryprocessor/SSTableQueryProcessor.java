/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.storage.sstable.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableQueryProcessor {

	/**
	 * The predicate to evaluate
	 */
	protected final Predicate predicate;
	
	/**
	 * The sstable manager
	 */
	protected final SSTableManager ssTableManager;

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
	protected final List<ReadOnlyTupleStorage> unprocessedStorages;

	/**
	 * The aquired storages
	 */
	protected final List<ReadOnlyTupleStorage> aquiredStorages;

	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(SSTableQueryProcessor.class);
	
	
	public SSTableQueryProcessor(final Predicate predicate, final SSTableManager ssTableManager) {
		this.predicate = predicate;
		this.ssTableManager = ssTableManager;
		this.ready = false;
		this.seenTuples = new HashMap<String, Long>();
		this.aquiredStorages = new LinkedList<ReadOnlyTupleStorage>();
		this.unprocessedStorages = new LinkedList<ReadOnlyTupleStorage>();
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
			protected ReadOnlyTupleStorage activeStorage = null;
			
			/**
			 * The next precomputed tuple
			 */
			protected Tuple nextTuple;
			
			protected void setupNewIterator() {
				activeIterator = null;
				activeStorage = null;

				// Find next iterator 
				while(! unprocessedStorages.isEmpty()) {
					activeStorage = unprocessedStorages.remove(0);
					activeIterator = activeStorage.getMatchingTuples(predicate);
					
					if(activeIterator.hasNext()) {
						return;
					}
				}
				
				activeIterator = null;
				activeStorage = null;
			}
			
			protected void setupNextTuple() throws StorageManagerException {
				if(ready == false) {
					throw new IllegalStateException("Iterator is not ready");
				}

				nextTuple = null;
				
				while(nextTuple == null) {
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
						if(oldTimestamp < possibleTuple.getTimestamp()) {
							logger.warn("Unprocessded: {}", unprocessedStorages);
							logger.warn("Aquired: {}", aquiredStorages);
							logger.warn("Got newer tuple {} than {}", possibleTuple, oldTimestamp);
							seenTuples.put(possibleTuple.getKey(), possibleTuple.getTimestamp());
						}
					} else {
						// Set nextTuple != null to exit the loop
						nextTuple = getMostRecentVersionForTuple(possibleTuple);
						seenTuples.put(possibleTuple.getKey(), possibleTuple.getTimestamp());
					}
				}
			}
			
			/**
			 * Get the most recent version of the tuple
			 * @param tuple
			 * @return
			 * @throws StorageManagerException 
			 */
			public Tuple getMostRecentVersionForTuple(final Tuple tuple) throws StorageManagerException {
				
				// Get the most recent version of the tuple
				// e.g. Memtables can contain multiple versions of the key
				// The iterator can return an outdated version
				Tuple resultTuple = activeStorage.get(tuple.getKey());
				
				for(final ReadOnlyTupleStorage readOnlyTupleStorage : unprocessedStorages) {
					if(TupleHelper.canStorageContainNewerTuple(resultTuple, readOnlyTupleStorage)) {
						final Tuple possibleTuple = readOnlyTupleStorage.get(tuple.getKey());
						resultTuple = TupleHelper.returnMostRecentTuple(resultTuple, possibleTuple);
					}
				}
				
				return resultTuple;
			}
			
			@Override
			public boolean hasNext() {
		
				try {
					if(nextTuple == null) {
						setupNextTuple();
					}
				} catch (StorageManagerException e) {
					logger.error("Got an exception while locating next tuple", e);
				}
				
				return nextTuple != null;
			}

			@Override
			public Tuple next() {

				if(ready == false) {
					throw new IllegalStateException("Iterator is not ready");
				}
				
				if(nextTuple == null) {
					throw new IllegalStateException("Next tuple is null, did you really call hasNext() before?");
				}
				
				final Tuple resultTuple = nextTuple;
				nextTuple = null;
				return resultTuple;
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
		ssTableManager.releaseStorage(aquiredStorages);
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
			aquiredStorages.addAll(ssTableManager.aquireStorage());
			
			unprocessedStorages.clear();
			unprocessedStorages.addAll(aquiredStorages);
	
			// Sort tables regarding the newest tuple timestamp 
			// The newest storage should be on top of the list
			unprocessedStorages.sort((storage1, storage2) 
					-> Long.compare(storage2.getNewestTupleTimestamp(), 
							        storage1.getNewestTupleTimestamp()));
			
			ready = true;
		
		} catch (StorageManagerException e) {
			logger.error("Unable to aquire tables", e);
			cleanup();
		}
	}
	
}

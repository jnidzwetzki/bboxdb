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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractQueryProcessor {
	
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
	
	public AbstractQueryProcessor(final TupleStoreManager tupleStoreManager) {
		this.tupleStoreManager = tupleStoreManager;
		this.ready = false;
		this.seenTuples = new HashMap<String, Long>();
		this.aquiredStorages = new LinkedList<ReadOnlyTupleStore>();
		this.unprocessedStorages = new LinkedList<ReadOnlyTupleStore>();
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
	protected List<Tuple> getVersionsForTuple(final Tuple tuple, final ReadOnlyTupleStore activeStorage) 
			throws StorageManagerException {
		
		final List<Tuple> resultTuples = getAllTupleVersionsForKey(tuple.getKey(), activeStorage);
		
		final TupleStoreConfiguration tupleStoreConfiguration 
			= tupleStoreManager.getTupleStoreConfiguration();
		
		final DuplicateResolver<Tuple> resolver 
			= TupleDuplicateResolverFactory.build(tupleStoreConfiguration);
		
		// Removed unwanted tuples for key
		resolver.removeDuplicates(resultTuples);
		
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
}

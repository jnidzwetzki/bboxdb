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
package org.bboxdb.storage.queryprocessor.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTablescanOperator implements Operator {
	
	/**
	 * The unprocessed storages
	 */
	protected final List<ReadOnlyTupleStore> unprocessedStorages;

	/**
	 * The aquired storages
	 */
	protected final List<ReadOnlyTupleStore> aquiredStorages;
	
	/**
	 * The sstable manager
	 */
	protected final TupleStoreManager tupleStoreManager;
	
	/**
	 * The seen tuples
	 */
	protected final Set<String> seenTuples;

	/**
	 * Is the iterator ready?
	 */
	protected boolean ready;

	/**
	 * The Logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(AbstractTablescanOperator.class);
	
	public AbstractTablescanOperator(final TupleStoreManager tupleStoreManager) {
		this.tupleStoreManager = tupleStoreManager;
		this.ready = false;
		this.aquiredStorages = new LinkedList<ReadOnlyTupleStore>();
		this.unprocessedStorages = new LinkedList<ReadOnlyTupleStore>();
		this.seenTuples = new HashSet<>();
	}
	
	/**
	 * Cleanup all aquired tables
	 */
	@Override
	public void close() {
		ready = false;
		tupleStoreManager.releaseStorage(aquiredStorages);
		aquiredStorages.clear();
		unprocessedStorages.clear();
		seenTuples.clear();
	}

	/**
	 * Prepare the unprocessed storage list
	 * @throws StorageManagerException 
	 */
	protected void aquireStorage() {
		try {
			close();
			aquiredStorages.addAll(tupleStoreManager.aquireStorage());			
			unprocessedStorages.addAll(aquiredStorages);
			ready = true;		
		} catch (StorageManagerException e) {
			logger.error("Unable to aquire tables", e);
			close();
		}
	}

	/**
	 * Get the tuple store name
	 * @return
	 */
	public TupleStoreName getTupleStoreName() {
		return tupleStoreManager.getTupleStoreName();
	}
	
	/**
	 * Setup the next tuplestore
	 * @param nextStorage 
	 * @return
	 */
	protected abstract Iterator<Tuple> setupNewTuplestore(final ReadOnlyTupleStore nextStorage);
	
	/**
	 * Filter the retrieved tuple versions
	 * @param tupleVersions
	 */
	protected abstract void filterTupleVersions(final List<Tuple> tupleVersions);
	
	public Iterator<JoinedTuple> iterator() {

		aquireStorage();
		
		return new Iterator<JoinedTuple>() {

			/**
			 * The active iterator
			 */
			protected Iterator<Tuple> activeIterator = null;
			
			/**
			 * The next precomputed tuple
			 */
			protected final List<JoinedTuple> nextTuples = new ArrayList<>();
			
			/**
			 * Setup the next iterator
			 */
			protected void setupNewIterator() {
				activeIterator = null;

				// Find next iterator 
				while(! unprocessedStorages.isEmpty()) {
					
					final ReadOnlyTupleStore nextStorage = unprocessedStorages.remove(0);
					activeIterator = setupNewTuplestore(nextStorage);
					
					if(activeIterator == null) {
						continue;
					}
					
					if(activeIterator.hasNext()) {
						return;
					}
				}
				
				activeIterator = null;
			}
			
			/**
			 * Fetch the next tuple from the iterator
			 * @throws StorageManagerException
			 */
			protected void setupNextTuples() throws StorageManagerException {
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
					
					if(! seenTuples.contains(possibleTuple.getKey())) {
						final List<Tuple> tupleVersions = tupleStoreManager.getVersionsForTuple(
								possibleTuple.getKey());
												
						filterTupleVersions(tupleVersions);
						
						final String tupelStorename = tupleStoreManager.getTupleStoreName().getFullnameWithoutPrefix();

						tupleVersions
							.stream()
							.map(t -> new JoinedTuple(t, tupelStorename))
							.forEach(t -> nextTuples.add(t));
						
						seenTuples.add(possibleTuple.getKey());
					}
				}
			}

			@Override
			public boolean hasNext() {
				try {
					if(nextTuples.isEmpty()) {
						setupNextTuples();
					}
				} catch (StorageManagerException e) {
					logger.error("Got an exception while locating next tuple", e);
				}
				
				return (! nextTuples.isEmpty());
			}

			@Override
			public JoinedTuple next() {

				if(ready == false) {
					throw new IllegalStateException("Iterator is not ready");
				}
				
				if(nextTuples.isEmpty()) {
					throw new IllegalStateException("Next tuple is empty, did you really call hasNext() before?");
				}
				
				return nextTuples.remove(0);
			}
		};
	}
}

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
package org.bboxdb.storage.tuplestore.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bboxdb.storage.memtable.Memtable;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;

/**
 * This class holds references to all known tuple storages (sstables, memtables) for 
 * a certain sstable 
 *
 */
public class TupleStoreInstanceManager {
	
	/**
	 * The active memtable
	 */
	protected Memtable memtable;
	
	/**
	 * The unflushed memtables
	 */
	protected final List<Memtable> unflushedMemtables;
	
	/**
	 * The reader for existing SSTables
	 */
	protected final List<SSTableFacade> sstableFacades;
	
	/**
	 * The state (read only / read write) of the manager
	 */
	protected volatile TupleStoreManagerState sstableManagerState;
	
	
	public TupleStoreInstanceManager() {		
		this.sstableFacades = new ArrayList<>();
		this.unflushedMemtables = new ArrayList<>();
		this.sstableManagerState = TupleStoreManagerState.READ_WRITE;
	}
	
	/**
	 * Activate a new memtable, the old memtable is transfered into the
	 * unflushed memtable list and also pushed into the flush queue
	 * 
	 * @param newMemtable
	 * @return 
	 */
	public synchronized Memtable activateNewMemtable(final Memtable newMemtable) {
		
		if(memtable != null) {
			unflushedMemtables.add(memtable);
		}	
		
		final Memtable oldMemtable = memtable;
		memtable = newMemtable;
		
		return oldMemtable;
	}
	
	/**
	 * After the flush, the memtable can be replaced with an sstable facade
	 * @param memtable
	 * @param sstableFacade
	 */
	public synchronized void replaceMemtableWithSSTable(final Memtable memtable, 
			final SSTableFacade sstableFacade) {
		
		//logger.debug("Replacing memtable {} with sstable {}", memtable, sstableFacade);
		
		// The memtable could be empty and no data was 
		// written to disk
		if(sstableFacade != null) {
			sstableFacades.add(sstableFacade);
		}
		
		final boolean removeResult = unflushedMemtables.remove(memtable);
		
		assert (removeResult == true) : "Unable to remove memtable from unflushed list: " 
				+ memtable + "/" + unflushedMemtables;
		
		// Notify waiter (e.g. the checkpoint thread)
		notifyAll();
	}
	
	/**
	 * Replace some compaced and merged sstables
	 * @param newSStable
	 * @param oldFacades
	 */
	public synchronized void replaceCompactedSStables(final List<SSTableFacade> newFacedes, 
			final List<SSTableFacade> oldFacades) {
		
		sstableFacades.addAll(newFacedes);
		final boolean removeResult = sstableFacades.removeAll(oldFacades);
		
		assert (removeResult == true) : "Unable to remove old facades in replaceCompactedSStables: " 
			+ oldFacades;
	}
	
	/**
	 * Add a new SSTable
	 * @param newSStable
	 */
	public synchronized void addNewDetectedSSTable(final SSTableFacade newSStable) {
		sstableFacades.add(newSStable);
	}
	
	/**
	 * Get a list with all active storages
	 * @return
	 */
	public synchronized List<ReadOnlyTupleStore> getAllTupleStorages() {
		final List<ReadOnlyTupleStore> allStorages = new ArrayList<>();
		
		allStorages.add(memtable);
		allStorages.addAll(unflushedMemtables);
		allStorages.addAll(sstableFacades);
		
		return allStorages;
	}
	
	/**
	 * Clear all known tuple stores
	 */
	public synchronized void clear() {
		memtable = null;
		sstableFacades.clear();
		unflushedMemtables.clear();
		notifyAll();
	}

	/**
	 * Get the active memtable
	 * @return
	 */
	public Memtable getMemtable() {
		return memtable;
	}
	
	/**
	 * Get the sstable facades
	 * @return
	 */
	public synchronized Collection<SSTableFacade> getSstableFacades() {
		return Collections.unmodifiableCollection(sstableFacades);
	}
	
	/**
	 * Get all in memory storages
	 * @return 
	 */
	public synchronized List<ReadOnlyTupleStore> getAllInMemoryStorages() {
		final List<ReadOnlyTupleStore> resultList = new ArrayList<ReadOnlyTupleStore>();
		resultList.add(memtable);
		resultList.addAll(unflushedMemtables);
		return resultList;
	}
	
	/**
	 * Wait until the memtable is flushed to disk
	 * @param memtable
	 * @throws InterruptedException 
	 */
	public synchronized void waitForMemtableFlush(final Memtable memtable) throws InterruptedException {
		while(unflushedMemtables.contains(memtable)) {
			wait();
		}
	}
	
	/**
	 * Wait until all memtables are flushed to disk
	 * @param memtable
	 * @throws InterruptedException 
	 */
	public synchronized void waitForAllMemtablesFlushed() throws InterruptedException {
		while(! unflushedMemtables.isEmpty()) {
			wait();
		}
	}
	
	/**
	 * Set to read only
	 */
	public synchronized void setReadOnly() {
		sstableManagerState = TupleStoreManagerState.READ_ONLY;
		notifyAll();
	}
	
	/**
	 * Set to read write
	 */
	public synchronized void setReadWrite() {
		sstableManagerState = TupleStoreManagerState.READ_WRITE;
		notifyAll();
	}
	
	/**
	 * Get the state
	 * @return
	 */
	public TupleStoreManagerState getState() {
		return sstableManagerState;
	}
}

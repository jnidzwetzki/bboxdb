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
package de.fernunihagen.dna.scalephant.storage.sstable.reader;

import de.fernunihagen.dna.scalephant.storage.Memtable;
import de.fernunihagen.dna.scalephant.storage.ReadOnlyTupleStorage;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.TupleHelper;

public class TupleByKeyLocator {

	/**
	 * The key to locate
	 */
	protected final String key;
	
	/**
	 * The sstable manager
	 * @param key
	 * @param sstableManager
	 */
	protected final SSTableManager sstableManager;
	
	/**
	 * The most recent version of the tuple
	 */
	protected Tuple mostRecentTuple;
	
	public TupleByKeyLocator(final String key, final SSTableManager sstableManager) {
		this.key = key;
		this.sstableManager = sstableManager;
		this.mostRecentTuple = null;
	}
	
	/**
	 * Get the most recent tuple for key
	 * @param key
	 * @return
	 * @throws StorageManagerException 
	 */
	public Tuple getMostRecentTuple() throws StorageManagerException {
		
		// Is tuple stored in the current memtable?
		mostRecentTuple = sstableManager.getMemtable().get(key);
		
		// Check unflushed memtables
		updateRecentTupleFromUnflushedMemtables();
				
		boolean readComplete = false;
		while(! readComplete) {
			readComplete = true;
		
			// Read data from the persistent SSTables
			for(final SSTableFacade facade : sstableManager.getSstableFacades()) {
				
				final boolean couldBeAquired = handleFacade(facade);
				if(! couldBeAquired) {
					readComplete = false;
					break;
				}
			}
		}
		
		return TupleHelper.replaceDeletedTupleWithNull(mostRecentTuple);
	}	
	
	/**
	 * Try to acquire the given facade and read the key
	 * @param facade
	 * @return
	 * @throws StorageManagerException
	 */
	protected boolean handleFacade(final SSTableFacade facade) throws StorageManagerException {
		final boolean canBeUsed = facade.acquire();
		
		if(! canBeUsed ) {
			return false;
		}
		
		try {
			if(canStorageContainNewerTuple(facade)) {
				final Tuple facadeTuple = facade.get(key);
				mostRecentTuple = TupleHelper.returnMostRecentTuple(mostRecentTuple, facadeTuple);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			facade.release();
		}
		
		return true;
	}
	
	
	/**
	 * Can the given storage contain a newer tuple than the recent tuple?
	 * @param memtable
	 * @return
	 */
	protected boolean canStorageContainNewerTuple(final ReadOnlyTupleStorage memtable) {
		if(mostRecentTuple == null) {
			return true;
		}
		
		if(memtable.getNewestTupleTimestamp() > mostRecentTuple.getTimestamp()) {
			return true;
		}
		
		return false;
	}
	

	
	/**
	 * Get the tuple from the unflushed memtables
	 * @param memtableRecentTuple 
	 * @param key
	 * @return
	 */
	protected void updateRecentTupleFromUnflushedMemtables() {
				
		for(final Memtable unflushedMemtable : sstableManager.getUnflushedMemtables()) {
			if(canStorageContainNewerTuple(unflushedMemtable)) {
				final Tuple unflushedMemtableTuple = unflushedMemtable.get(key);	
				mostRecentTuple = TupleHelper.returnMostRecentTuple(mostRecentTuple, unflushedMemtableTuple);
			}
		}	
	}

}

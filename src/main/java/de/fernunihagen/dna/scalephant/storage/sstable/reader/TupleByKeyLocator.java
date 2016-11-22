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
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableHelper;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;

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
		// Read unflushed memtables first
		mostRecentTuple = getTupleFromUnflushedMemtables();
				
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
		
		return SSTableHelper.replaceDeletedTupleWithNull(mostRecentTuple);
	}	
	
	/**
	 * Try to aquire the given facade and read the key
	 * @param facade
	 * @return
	 * @throws StorageManagerException
	 */
	protected boolean handleFacade(final SSTableFacade facade) throws StorageManagerException {
		boolean canBeUsed = facade.acquire();
		
		if(! canBeUsed ) {
			return false;
		}
		
		try {
			if(canContainNewerTuple(facade)) {
				final Tuple facadeTuple = getTupleFromFacade(key, facade);
				mostRecentTuple = SSTableHelper.returnMostRecentTuple(mostRecentTuple, facadeTuple);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			facade.release();
		}
		
		return true;
	}
	
	/**
	 * Can the given face contin a more recent version for the tuple?
	 * @param facade
	 * @param tuple
	 * @return
	 */
	protected boolean canContainNewerTuple(final SSTableFacade facade) {
		if(mostRecentTuple == null) {
			return true;
		}
		
		if(facade.getSsTableMetadata().getNewestTuple() > mostRecentTuple.getTimestamp()) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Get the tuple for key from the given facade
	 * @param key
	 * @param facade
	 * @return
	 * @throws StorageManagerException
	 */
	protected Tuple getTupleFromFacade(final String key, final SSTableFacade facade) throws StorageManagerException {
		final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
		final SSTableReader reader = facade.getSsTableReader();
		
		final int position = indexReader.getPositionForTuple(key);
		
		// Does the tuple exist?
		if(position == -1) {
			return null;
		}
		
		return reader.getTupleAtPosition(position);
	}
	
	/**
	 * Get the tuple from the unflushed memtables
	 * @param key
	 * @return
	 */
	protected Tuple getTupleFromUnflushedMemtables() {
		
		Tuple mostRecentTuple = null;
		
		for(final Memtable unflushedMemtable : sstableManager.getUnflushedMemtables()) {
			final Tuple memtableTuple = unflushedMemtable.get(key);
			mostRecentTuple = SSTableHelper.returnMostRecentTuple(mostRecentTuple, memtableTuple);
		}
		
		return mostRecentTuple;
	}

}

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
	 * The sstable manager
	 * @param key
	 * @param sstableManager
	 */
	protected final SSTableManager sstableManager;
	
	public TupleByKeyLocator(final SSTableManager sstableManager) {
		this.sstableManager = sstableManager;
	}
	
	/**
	 * Get the most recent tuple for key
	 * @param key
	 * @return
	 * @throws StorageManagerException 
	 */
	public Tuple getMostRecentTuple(final String key) throws StorageManagerException {
		// Read unflushed memtables first
		Tuple tuple = getTupleFromUnflushedMemtables(key);
				
		boolean readComplete = false;
		while(! readComplete) {
			readComplete = true;
		
			// Read data from the persistent SSTables
			for(final SSTableFacade facade : sstableManager.getSstableFacades()) {
				boolean canBeUsed = facade.acquire();
				
				if(! canBeUsed ) {
					readComplete = false;
					break;
				}
				
				final SSTableKeyIndexReader indexReader = facade.getSsTableKeyIndexReader();
				final SSTableReader reader = facade.getSsTableReader();
				
				final int position = indexReader.getPositionForTuple(key);
				
				// Found a tuple
				if(position != -1) {
					final Tuple tableTuple = reader.getTupleAtPosition(position);
					if(tuple == null) {
						tuple = tableTuple;
					} else if(tableTuple.getTimestamp() > tuple.getTimestamp()) {
						tuple = tableTuple;
					}
				}
				
				facade.release();
			}
		}
		
		return SSTableHelper.replaceDeletedTupleWithNull(tuple);
	}	
	
	/**
	 * Get the tuple from the unflushed memtables
	 * @param key
	 * @return
	 */
	protected Tuple getTupleFromUnflushedMemtables(final String key) {
		
		Tuple result = null;
		
		for(final Memtable unflushedMemtable : sstableManager.getUnflushedMemtables()) {
			final Tuple tuple = unflushedMemtable.get(key);
			
			if(tuple != null) {
				if(result == null) {
					result = tuple;
					continue;
				}
				
				// Get the most recent version of the tuple
				if(tuple.compareTo(result) < 0) {
					result = tuple;
					continue;	
				}
			}
		}
		
		return result;
	}

}

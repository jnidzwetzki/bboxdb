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
package de.fernunihagen.dna.scalephant.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.TupleHelper;

public class Memtable implements ScalephantService, Storage, Iterable<Tuple> {
	
	/**
	 * The name of the corresponding table
	 */
	protected final SSTableName table;
	
	/**
	 * The memtable
	 */
	protected final Tuple[] data;
	
	/**
	 * The next free position in the data array
	 */
	protected int freePos;
	
	/**
	 * Maximal number of entries keep in memory
	 */
	protected final int maxEntries;
	
	/**
	 * Maximal size of memtable in KB
	 */
	protected final int maxSizeInMemory;
	
	/**
	 * Current memory size in KB
	 */
	protected int sizeInMemory;
	
	/**
	 * The timestamp when the memtable is created
	 */
	protected long createdTimestamp;
	
	/**
	 * The oldest tuple
	 */
	protected long oldestTupleTimestamp;
	
	/**
	 * The newest tuple
	 */
	protected long newestTupleTimestamp;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Memtable.class);
	
	public Memtable(final SSTableName table, final int entries, final int maxSizeInMemory) {
		this.table = table;
		this.maxEntries = entries;
		this.maxSizeInMemory = maxSizeInMemory;
		
		this.data = new Tuple[entries];
		this.freePos = -1;
		this.sizeInMemory = 0;
		
		this.createdTimestamp = System.currentTimeMillis();
		this.oldestTupleTimestamp = -1;
		this.newestTupleTimestamp = -1;
	}

	@Override
	public void init() {
		if(freePos != -1) {
			logger.error("init() called on an initalized memtable");
			return;
		}
		
		logger.info("Initializing a new memtable for table: " + table.getFullname());
		freePos = 0;
	}

	@Override
	public void shutdown() {
		
	}

	@Override
	public void put(final Tuple value) throws StorageManagerException {
		
		if(freePos >= maxEntries) {
			throw new StorageManagerException("Unable to store a new tuple, all memtable slots are full");
		}

		data[freePos] = value;
		freePos++;
		sizeInMemory = sizeInMemory + value.getSize();
		
		if(oldestTupleTimestamp == -1) {
			oldestTupleTimestamp = value.getTimestamp();
		} else {
			oldestTupleTimestamp = Math.min(oldestTupleTimestamp, value.getTimestamp());
		}
		
		if(newestTupleTimestamp == -1) {
			newestTupleTimestamp = value.getTimestamp();
		} else {
			newestTupleTimestamp = Math.max(newestTupleTimestamp, value.getTimestamp());
		}
	}

	/**
	 * Get the most recent version of the tuple for key
	 * 
	 */
	@Override
	public Tuple get(final String key) {
		
		Tuple mostRecentTuple = null;
		
		for(int i = 0; i < freePos; i++) {
			final Tuple possibleTuple = data[i];
			
			if(possibleTuple != null && possibleTuple.getKey().equals(key)) {
				mostRecentTuple = TupleHelper.returnMostRecentTuple(mostRecentTuple, possibleTuple);
			}
		}
		
		return mostRecentTuple;
	}
	
	/**
	 * Get all tuples that are inside of the bounding box. The result list may 
	 * contain (outdated) tuples with the same key.
	 * 
	 */
	@Override
	public Collection<Tuple> getTuplesInside(final BoundingBox boundingBox)
			throws StorageManagerException {
		
		final List<Tuple> resultList = new ArrayList<Tuple>();
		
		for(int i = 0; i < freePos; i++) {
			final Tuple possibleTuple = data[i];
			if(possibleTuple.getBoundingBox().overlaps(boundingBox)) {
				resultList.add(possibleTuple);
			}
		}
		
		return resultList;
	}
	
	/**
	 * Returns all tuples that are inserted after a certain time stamp
	 */
	@Override
	public Collection<Tuple> getTuplesAfterTime(final long timestamp)
			throws StorageManagerException {
		
		final List<Tuple> resultList = new ArrayList<Tuple>();
		
		for(int i = 0; i < freePos; i++) {
			final Tuple possibleTuple = data[i];
			if(possibleTuple.getTimestamp() > timestamp) {
				resultList.add(possibleTuple);
			}
		}
		
		return resultList;
	}
	
	/**
	 * Delete a tuple, this is implemented by inserting a DeletedTuple object
	 *
	 */
	@Override
	public void delete(final String key, final long timestamp) throws StorageManagerException {
		final Tuple deleteTuple = new DeletedTuple(key, timestamp);
		put(deleteTuple);
	}
	

	/**
	 * Get a sorted list with all recent tuples
	 * @return 
	 * 
	 */
	public List<Tuple> getSortedTupleList() {
		final SortedMap<String, Tuple> allTuples = new TreeMap<String, Tuple>();
		
		for(int i = 0; i < freePos; i++) {
			final String key = data[i].getKey();
			
			if(! allTuples.containsKey(key)) {
				allTuples.put(key, data[i]);
			} else {
				if(allTuples.get(key).getTimestamp() < data[i].getTimestamp()) {
					
					if(data[i] instanceof DeletedTuple) {
						allTuples.remove(key);
					} else {
						allTuples.put(key, data[i]);
					}
				}
			}
		}
		
		final List<Tuple> resultList = new ArrayList<Tuple>(allTuples.size());
		resultList.addAll(allTuples.values());
		
		return resultList;
	}
	
	/**
	 * Clean the whole memtable, useful for testing
	 * 
	 */
	@Override
	public void clear() {
		logger.info("Clear on memtable " + table + " called");
		
		for(int i = 0; i < data.length; i++) {
			data[i] = null;
		}
		
		freePos = 0;
	}
	
	/**
	 * Is this memtable full and needs to be flushed to disk
	 * 
	 * @return
	 */
	public boolean isFull() {
		
		// Check size of the table
		if(sizeInMemory >= maxSizeInMemory) {
			return true;
		}
		
		// Check number of entries
		if(freePos + 1 > maxEntries) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Is this memtable empty?
	 */
	public boolean isEmpty() {
		if(freePos <= 0) {
			return true;
		}
		
		return false;
	}

	/**
	 * Get the maximal number of entries in the memtable
	 * @return
	 */
	public int getMaxEntries() {
		return maxEntries;
	}

	/**
	 * The size of the memtable in memory
	 * @return
	 */
	public int getSizeInMemory() {
		return sizeInMemory;
	}
	
	/**
	 * Get the created timestamp
	 * @return
	 */
	public long getCreatedTimestamp() {
		return createdTimestamp;
	}
	
	@Override
	public String getServicename() {
		return "Memtable";
	}

	@Override
	public Iterator<Tuple> iterator() {
		return new Iterator<Tuple>() {

			protected int entry = 0;
			protected int lastEntry = freePos;
			
			@Override
			public boolean hasNext() {
				return entry < lastEntry;
			}

			@Override
			public Tuple next() {
				
				if(entry > lastEntry) {
					throw new IllegalStateException("Requesting wrong position: " + entry + " of " + lastEntry);
				}
				
				final Tuple tuple = data[entry];
				entry++;
				return tuple;
			}

			@Override
			public void remove() {
				throw new IllegalStateException("Remove is not supported");
			}
		};
	}

	/**
	 * Get the oldest tuple timestamp
	 * @return
	 */
	public long getOldestTupleTimestamp() {
		return oldestTupleTimestamp;
	}

	/**
	 * Get the newest tuple timestamp
	 * @return
	 */
	public long getNewestTupleTimestamp() {
		return newestTupleTimestamp;
	}

}

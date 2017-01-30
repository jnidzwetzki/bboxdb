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
package org.bboxdb.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.BBoxDBService;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.TupleHelper;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndex;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;

public class Memtable implements BBoxDBService, ReadWriteTupleStorage {
	
	/**
	 * The name of the corresponding table
	 */
	protected final SSTableName table;
	
	/**
	 * The memtable
	 */
	protected final Tuple[] data;
	
	/**
	 * The bloom filter
	 */
	protected final BloomFilter<String> bloomFilter;
	
	/**
	 * The spatial index
	 */
	protected final SpatialIndex spatialIndex;
	
	/**
	 * The next free position in the data array
	 */
	protected int freePos;
	
	/**
	 * Maximal number of entries keep in memory
	 */
	protected final int maxEntries;
	
	/**
	 * Maximal size of memtable in bytes
	 */
	protected final long maxSizeInMemory;
	
	/**
	 * Current memory size in bytes
	 */
	protected long sizeInMemory;
	
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
	 * The reference counter
	 */
	protected final AtomicInteger usage;
	
	/**
	 * Is a deletion performed after (usage == 0)
	 */
	protected boolean pendingDelete;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Memtable.class);
	
	public Memtable(final SSTableName table, final int entries, final long maxSizeInMemory) {
		this.table = table;
		this.maxEntries = entries;
		this.maxSizeInMemory = maxSizeInMemory;
		
		this.data = new Tuple[entries];
		this.freePos = -1;
		this.sizeInMemory = 0;
		
		this.bloomFilter = BloomFilterBuilder.buildBloomFilter(entries);
		this.spatialIndex = SpatialIndexFactory.getInstance();
		
		this.createdTimestamp = System.currentTimeMillis();
		this.oldestTupleTimestamp = -1;
		this.newestTupleTimestamp = -1;
		
		this.usage = new AtomicInteger(0);
		this.pendingDelete = false;
	}

	@Override
	public void init() {
		if(freePos != -1) {
			logger.error("init() called on an initalized memtable");
			return;
		}
		
		logger.debug("Initializing a new memtable for table: {}", table.getFullname());
		freePos = 0;
	}

	@Override
	public void shutdown() {
		
	}

	@Override
	public void put(final Tuple value) throws StorageManagerException {
		
		assert (usage.get() > 0);
		
		if(freePos >= maxEntries) {
			throw new StorageManagerException("Unable to store a new tuple, all memtable slots are full");
		}

		data[freePos] = value;
		bloomFilter.put(value.getKey());
		spatialIndex.insert(new SpatialIndexEntry(value.getKey(), value.getBoundingBox()));
		
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
		
		assert (usage.get() > 0);
		
		// The element is not contained in the bloom filter
		if(! bloomFilter.mightContain(key)) {
			return null;
		}
		
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
	 * Delete a tuple, this is implemented by inserting a DeletedTuple object
	 *
	 */
	@Override
	public void delete(final String key, final long timestamp) throws StorageManagerException {
		assert (usage.get() > 0);

		final Tuple deleteTuple = new DeletedTuple(key, timestamp);
		put(deleteTuple);
	}

	/**
	 * Get a sorted list with all recent tuples
	 * @return 
	 * 
	 */
	public List<Tuple> getSortedTupleList() {
		assert (usage.get() > 0);

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
		logger.debug("Clear on memtable {} called", table);
		
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
	 * Get the total amount of entries in the table
	 * @return
	 */
	public int getTotalEntries() {
		return freePos;
	}

	/**
	 * The size of the memtable in memory
	 * @return
	 */
	@Override
	public long getSize() {
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

		assert (usage.get() > 0);

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
	@Override
	public long getOldestTupleTimestamp() {
		return oldestTupleTimestamp;
	}

	/**
	 * Get the newest tuple timestamp
	 * @return
	 */
	@Override
	public long getNewestTupleTimestamp() {
		return newestTupleTimestamp;
	}

	@Override
	public void deleteOnClose() {
		logger.debug("deleteOnClose called and we have {} references", usage.get());

		pendingDelete = true;
		
		if(usage.get() == 0) {
			clear();
		}
	}

	@Override
	public boolean acquire() {
		if(pendingDelete == true) {
			return false;
		}
		
		usage.incrementAndGet();
		return true;
	}

	@Override
	public void release() {
		assert (usage.get() > 0);

		usage.decrementAndGet();
		
		if(pendingDelete) {
			logger.debug("Release called and we have {} references", usage.get());

			if(usage.get() == 0) {
				clear();
			}
		}
	}

	@Override
	public String getInternalName() {
		return table.getFullname() + " / " + createdTimestamp;
	}
	
	@Override
	public SSTableName getSStableName() {
		return table;
	}
	
	@Override
	public long getNumberOfTuples() {
		assert (usage.get() > 0);

		return freePos;
	}

	@Override
	public Tuple getTupleAtPosition(final long position) {		
		assert (usage.get() > 0);

		return data[(int) position];
	}

	@Override
	public String toString() {
		return "Memtable [table=" + table.getFullname() + ", freePos=" + freePos
				+ ", sizeInMemory=" + sizeInMemory + ", createdTimestamp="
				+ createdTimestamp + ", oldestTupleTimestamp="
				+ oldestTupleTimestamp + ", newestTupleTimestamp="
				+ newestTupleTimestamp + "]";
	}

	@Override
	public Iterator<Tuple> getAllTuplesInBoundingBox(final BoundingBox boundingBox) {
		assert (usage.get() > 0);

		final List<? extends SpatialIndexEntry> matchingKeys = spatialIndex.getEntriesForRegion(boundingBox);
		@SuppressWarnings("rawtypes")
		final Iterator keyIterator = matchingKeys.iterator();
		
		return new Iterator<Tuple>() {

			@Override
			public boolean hasNext() {
				return keyIterator.hasNext();
			}

			@Override
			public Tuple next() {
				final SpatialIndexEntry entry = (SpatialIndexEntry) keyIterator.next();
				final String key = entry.getKey();
				return get(key);
			}
		};
	}

	
}

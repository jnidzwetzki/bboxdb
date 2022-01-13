/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.storage.memtable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilderFactory;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.tuplestore.ReadWriteTupleStore;
import org.bboxdb.storage.util.TupleHelper;
import org.bboxdb.storage.wal.WriteAheadLogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Memtable implements BBoxDBService, ReadWriteTupleStore {

	/**
	 * The name of the corresponding table
	 */
	private final TupleStoreName table;

	/**
	 * The memtable
	 */
	private final Tuple[] data;

	/**
	 * The spatial index
	 */
	private final SpatialIndexBuilder spatialIndexBuilder;

	/**
	 * The next free position in the data array
	 */
	private int freePos;

	/**
	 * Maximal number of entries keep in memory
	 */
	private final int maxEntries;

	/**
	 * Maximal size of memtable in bytes
	 */
	private final long maxSizeInMemory;

	/**
	 * Current memory size in bytes
	 */
	private long sizeInMemory;

	/**
	 * The timestamp when the memtable is created
	 */
	private long createdTimestamp;

	/**
	 * The oldest tuple
	 */
	private long oldestTupleTimestamp;

	/**
	 * The newest tuple
	 */
	private long newestTupleTimestamp;

	/**
	 * The reference counter
	 */
	private final AtomicInteger usage;

	/**
	 * Is a deletion performed after (usage == 0)
	 */
	private boolean pendingDelete;

	/**
	 * The write ahead log writer
	 */
	private final WriteAheadLogWriter walWriter;

	/**
	 * The key position map
	 */
	private final Map<String, Set<Integer>> keyPositions;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Memtable.class);

	public Memtable(final TupleStoreName table, final int entries, final long maxSizeInMemory,
			final WriteAheadLogWriter walWriter) {

		this.table = table;
		this.maxEntries = entries;
		this.maxSizeInMemory = maxSizeInMemory;
		this.walWriter = walWriter;

		this.data = new Tuple[entries];
		this.freePos = -1;
		this.sizeInMemory = 0;

		this.spatialIndexBuilder = SpatialIndexBuilderFactory.getInstance();

		this.createdTimestamp = System.currentTimeMillis();
		this.oldestTupleTimestamp = -1;
		this.newestTupleTimestamp = -1;

		this.usage = new AtomicInteger(0);
		this.pendingDelete = false;
		this.keyPositions = new HashMap<>();
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
	public void put(final Tuple tuple) throws StorageManagerException {

		assert (usage.get() > 0);

		if(freePos >= maxEntries) {
			throw new StorageManagerException("Unable to store a new tuple, all memtable slots are full");
		}

		if(walWriter != null) {
			walWriter.addTuple(tuple);
		}

		data[freePos] = tuple;
		
		if(! TupleHelper.isDeletedTuple(tuple)) {
			final SpatialIndexEntry indexEntry = new SpatialIndexEntry(tuple.getBoundingBox(), freePos);
			spatialIndexBuilder.insert(indexEntry);
		}

		keyPositions.computeIfAbsent(tuple.getKey(), (e) -> ConcurrentHashMap.newKeySet()).add(freePos);

		freePos++;
		sizeInMemory = sizeInMemory + tuple.getSize();

		if(oldestTupleTimestamp == -1) {
			oldestTupleTimestamp = tuple.getVersionTimestamp();
		} else {
			oldestTupleTimestamp = Math.min(oldestTupleTimestamp, tuple.getVersionTimestamp());
		}

		if(newestTupleTimestamp == -1) {
			newestTupleTimestamp = tuple.getVersionTimestamp();
		} else {
			newestTupleTimestamp = Math.max(newestTupleTimestamp, tuple.getVersionTimestamp());
		}
	}

	/**
	 * Get the most recent version of the tuple for key
	 *
	 */
	@Override
	public List<Tuple> get(final String key) {

		assert (usage.get() > 0) : "Usage is 0";

		final List<Tuple> resultList = new ArrayList<>();
		final Set<Integer> positions = keyPositions.get(key);

		if(positions == null || positions.isEmpty()) {
			return resultList;
		}

		for(final int pos : positions) {
			resultList.add(data[pos]);
		}

		return resultList;
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

		final List<Tuple> resultList = new ArrayList<>(freePos + 1);

		for(int i = 0; i < freePos; i++) {
			resultList.add(data[i]);
		}

		resultList.sort(TupleHelper.TUPLE_KEY_AND_VERSION_COMPARATOR);

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

		this.freePos = 0;
		this.sizeInMemory = 0;
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

			private int entry = 0;
			private int lastEntry = freePos;

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

	@Override
	public long getNewestTupleInsertedTimestamp() {
		if(freePos == 0) {
			return System.currentTimeMillis();
		}

		final Tuple mostRecentTuple = data[freePos - 1];
		return mostRecentTuple.getReceivedTimestamp();
	}

	/**
	 * Get the oldest tuple timestamp
	 * @return
	 */
	@Override
	public long getOldestTupleVersionTimestamp() {
		return oldestTupleTimestamp;
	}

	/**
	 * Get the newest tuple timestamp
	 * @return
	 */
	@Override
	public long getNewestTupleVersionTimestamp() {
		return newestTupleTimestamp;
	}

	@Override
	public void deleteOnClose() {
		logger.debug("deleteOnClose called and we have {} references", usage.get());

		pendingDelete = true;

		clearIfUnreferenced();
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
			clearIfUnreferenced();
		}
	}

	/**
	 * Clear if no other references are hold
	 */
	private void clearIfUnreferenced() {
		logger.debug("Release called and we have {} references", usage.get());

		if(usage.get() == 0) {
			clear();
		}

		try {
			if(walWriter != null) {
				walWriter.close();
				walWriter.deleteFile();
			}
		} catch (IOException e) {
			logger.error("Got exception while closing WAL", e);
		}
	}

	@Override
	public String getInternalName() {
		return table.getFullname() + " / " + createdTimestamp;
	}

	@Override
	public TupleStoreName getTupleStoreName() {
		return table;
	}

	@Override
	public long getNumberOfTuples() {
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
				+ newestTupleTimestamp +", pendingDelete=" + pendingDelete + "]";
	}

	@Override
	public Iterator<Tuple> getAllTuplesInBoundingBox(final Hyperrectangle boundingBox) {
		assert (usage.get() > 0);

		final List<? extends SpatialIndexEntry> matchingKeys = spatialIndexBuilder.getEntriesForRegion(boundingBox);

		final Iterator<? extends SpatialIndexEntry> keyIterator = matchingKeys.iterator();

		return new Iterator<Tuple>() {

			@Override
			public boolean hasNext() {
				return keyIterator.hasNext();
			}

			@Override
			public Tuple next() {
				final SpatialIndexEntry entry = keyIterator.next();
				final int pos = (int) entry.getValue();
				return data[pos];
			}
		};
	}

	@Override
	public boolean isPersistent() {
		return false;
	}

	@Override
	public boolean isDeletePending() {
		return pendingDelete;
	}

	/**
	 * Get the spatial index builder
	 * @return
	 */
	public SpatialIndexBuilder getSpatialIndexBuilder() {
		return spatialIndexBuilder;
	}
}

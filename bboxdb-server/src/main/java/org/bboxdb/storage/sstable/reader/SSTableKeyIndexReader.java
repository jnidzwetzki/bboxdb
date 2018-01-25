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
package org.bboxdb.storage.sstable.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class SSTableKeyIndexReader extends AbstractTableReader implements Iterable<Tuple> {
	
	/**
	 * The corresponding sstable reader
	 */
	protected final SSTableReader sstableReader;
	
	/**
	 * The key cache <Tuple Number, Key>
	 */
	protected LoadingCache<Long, String> keyCache;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(SSTableKeyIndexReader.class);

	public SSTableKeyIndexReader(final SSTableReader sstableReader) throws StorageManagerException {
		super(sstableReader.getDirectory(), sstableReader.getName(), sstableReader.getTablebumber());
		this.sstableReader = sstableReader;
	}

	@Override
	public void init() {
		super.init();
		logger.info("Opened index for relation: {} with {} entries", name.getFullname(), getNumberOfEntries());
	}

	/**
	 * Active the key cache with the given capacity
	 * @param elements
	 */
	public void activateKeyCache(final int elements) {
		
		if(keyCache != null) {
			throw new RuntimeException("Keycache was already be initiliazed");
		}
		
		// Don't activate the cache
		if(elements == 0) {
			return;
		}
		
		keyCache = CacheBuilder.newBuilder()
				.maximumSize(elements)
				.build(new CacheLoader<Long, String>() {

			@Override
			public String load(final Long tupleNumber) throws Exception {
				return readKeyFromBytePos(tupleNumber);
			}
			
		});
	}
	
	/**
	 * Scan the index file for the tuple position
	 * @param key
	 * @return
	 * @throws StorageManagerException 
	 */
	public List<Integer> getPositionsForTuple(final String key) throws StorageManagerException {

		try {
			int firstEntry = 0;
			int lastEntry = getNumberOfEntries() - 1;
			
			// Check key is > then first value
			final String firstValue = getKeyForIndexEntry(firstEntry);
			if(firstValue.equals(key)) {
				return fillKeyPositionArrayFromIndexEntry(key, firstEntry);
			}
			
			// Not found
			if(firstValue.compareTo(key) > 0) {
				return new ArrayList<>();
			}
			
			// Check if key is < then first value
			final String lastValue = getKeyForIndexEntry(lastEntry);
			if(lastValue.equals(key)) {
				return fillKeyPositionArrayFromIndexEntry(key, lastEntry);
			}
			
			// Not found
			if(lastValue.compareTo(key) < 0) {
				return new ArrayList<>();
			}

			// Binary search for key
			do {
				int curEntry = (int) ((lastEntry - firstEntry) / 2.0) + firstEntry;
				
/*				if(logger.isDebugEnabled()) {
					logger.debug("Low: " + firstEntry + " Up: " + lastEntry + " Pos: " + curEntry);
				}*/
				
				final String curEntryValue = getKeyForIndexEntry(curEntry);
				
				if(curEntryValue.equals(key)) {
					return fillKeyPositionArrayFromIndexEntry(key, curEntry);
				}
				
				if(key.compareTo(curEntryValue) > 0) {
					firstEntry = curEntry + 1;
				} else {
					lastEntry = curEntry - 1;
				}
				
			} while(firstEntry <= lastEntry);

		} catch (IOException e) {
			throw new StorageManagerException("Error while reading index file", e);
		}
		
		// Not found during binary scan
		return new ArrayList<>();
	}
	
	/**
	 * The SSTable can contain duplicates, so we nee to scan up and down from 
	 * the given position to retrive all keys
	 * 
	 * @param indexEntry
	 * @throws StorageManagerException 
	 * @throws IOException 
	 */
	protected List<Integer> fillKeyPositionArrayFromIndexEntry(final String key, final int indexEntry) 
			throws IOException, StorageManagerException {
		
		final List<Integer> resultList = new ArrayList<>();
		final int lastEntry = getNumberOfEntries() - 1;

		resultList.add(indexEntry);
		
		// Scan upper index entries
		int indexEntryTest = indexEntry + 1;
		while(indexEntryTest <= lastEntry) {
			final String curEntryValue = getKeyForIndexEntry(indexEntryTest);
			
			if(curEntryValue.equals(key)) {
				resultList.add(indexEntryTest);
			} else {
				break;
			}
			
			indexEntryTest++;
		}
		
		// Scan lower index entries
		indexEntryTest = indexEntry - 1;
				
		while(indexEntryTest >= 0) {
			final String curEntryValue = getKeyForIndexEntry(indexEntryTest);
			
			if(curEntryValue.equals(key)) {
				resultList.add(indexEntryTest);
			} else {
				break;
			}
			
			indexEntryTest--;
		}
		
		// Convert index positions
		return resultList.stream()
			.map((e) -> convertEntryToPosition(e))
			.collect(Collectors.toList());
	}

	/**
	 * Get the string key for index entry
	 * @param entry
	 * @return
	 * @throws IOException
	 * @throws StorageManagerException 
	 */
	public String getKeyForIndexEntry(final long entry) throws IOException, StorageManagerException {
		
		if(keyCache != null) {
			try {
				return keyCache.get(entry);
			} catch (ExecutionException e) {
				throw new StorageManagerException(e);
			}
		}
		
		return readKeyFromBytePos(entry);
	}

	/**
	 * Read the key from byte position
	 * @param entry
	 * @return
	 * @throws IOException
	 */
	protected String readKeyFromBytePos(final long entry) throws IOException {
		final int position = convertEntryToPosition(entry);
		return sstableReader.decodeOnlyKeyFromTupleAtPosition(position);
	}
	
	/**
	 * Get the tuple at the given position
	 * @param entry
	 * @return
	 * @throws IOException
	 * @throws StorageManagerException 
	 */
	public Tuple getTupleForIndexEntry(final long entry) throws IOException, StorageManagerException {
		final int position = convertEntryToPosition(entry);
		return sstableReader.getTupleAtPosition(position);
	}

	/**
	 * Convert the index entry to index file position
	 * @param entry
	 * @return
	 */
	protected synchronized int convertEntryToPosition(final long entry) {
		
		// Memory was unmapped
		if(memory == null) {
			return -1;
		}
		
		final byte[] magicBytes = getMagicBytes();
		
		memory.position((int) ((entry * SSTableConst.INDEX_ENTRY_BYTES) + magicBytes.length));
		int position = memory.getInt();
		return position;
	}

	/**
	 * Get the total number of entries
	 * @return
	 */
	public int getNumberOfEntries() {
		try {
			if(fileChannel == null) {
				logger.warn("getNumberOfEntries() called on closed sstableindexreader for relation: {}", name);
				return 0;
			}
			
			final byte[] magicBytes = getMagicBytes();
			
			return (int) ((fileChannel.size() - magicBytes.length) / SSTableConst.INDEX_ENTRY_BYTES);
		} catch (IOException e) {
			logger.error("IO Exception while reading from index", e);
		}
		
		return 0;
	}

	/**
	 * Getter for sstable reader
	 * @return
	 */
	public SSTableReader getSstableReader() {
		return sstableReader;
	}
	
	/**
	 * Iterate over the tuples in the sstable
	 */
	@Override
	public Iterator<Tuple> iterator() {
		
		return new Iterator<Tuple>() {

			protected int entry = 0;
			protected int lastEntry = getNumberOfEntries() - 1;
			
			@Override
			public boolean hasNext() {
				return entry <= lastEntry;
			}

			@Override
			public Tuple next() {
				
				if(entry > lastEntry) {
					throw new IllegalStateException("Requesting wrong position: " + entry + " of " + lastEntry);
				}
				
				try {
					final Tuple tuple = sstableReader.getTupleAtPosition(convertEntryToPosition(entry));
					entry++;
					return tuple;
				} catch (StorageManagerException e) {
					logger.error("Got exception while iterating (requesting entry " + (entry - 1) + " of " + lastEntry + ")", e);
				}
								
				return null;
			}

			@Override
			public void remove() {
				throw new IllegalStateException("Remove is not supported");
			}
		};
	}
	
	@Override
	public String getServicename() {
		return "SSTable key index reader";
	}
	
	/**
	 * Construct the filename of the index file
	 * @param sstableFacades
	 * @return
	 */
	@Override
	protected File constructFileToRead() {
		final String filename = SSTableHelper.getSSTableIndexFilename(directory, name, tablebumber);
		return new File(filename);
	}

	@Override
	protected byte[] getMagicBytes() {
		return SSTableConst.MAGIC_BYTES_INDEX;
	}
}
package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class SSTableIndexReader extends AbstractTableReader implements Iterable<Tuple> {
	
	/**
	 * The coresponding sstable reader
	 */
	protected final SSTableReader sstableReader;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(SSTableIndexReader.class);

	public SSTableIndexReader(final SSTableReader sstableReader) throws StorageManagerException {
		super(sstableReader.getName(), sstableReader.getDirectory(), constructFileFromReader(sstableReader));
		this.sstableReader = sstableReader;
	}

	/**
	 * Construct the filename of the index file
	 * @param sstableReader
	 * @return
	 */
	protected static File constructFileFromReader(
			final SSTableReader sstableReader) {
		
		return new File(SSTableManager.getSSTableIndexFilename(
				sstableReader.getDirectory(), 
				sstableReader.getName(), 
				sstableReader.getTablebumber()));
	}

	@Override
	public void init() {
		super.init();
		logger.info("Opened index for relation: " + name + " with " + getNumberOfEntries() + " entries");
	}

	/**
	 * Scan the index file for the tuple position
	 * @param key
	 * @return
	 * @throws StorageManagerException 
	 */
	public int getPositionForTuple(final String key) throws StorageManagerException {
		
		try {
			
			int firstEntry = 0;
			int lastEntry = getNumberOfEntries() - 1;
			
			// Check key is > then first value
			final String firstValue = getKeyForIndexEntry(firstEntry);
			if(firstValue.equals(key)) {
				return convertEntryToPosition(firstEntry);
			}
			
			if(firstValue.compareTo(key) > 0) {
				return -1;
			}
			
			// Check if key is < then first value
			final String lastValue = getKeyForIndexEntry(lastEntry);
			if(lastValue.equals(key)) {
				return convertEntryToPosition(lastEntry);
			}
			if(lastValue.compareTo(key) < 0) {
				return -1;
			}

			// Binary search for key
			do {
				int curEntry = (int) ((lastEntry - firstEntry) / 2.0) + firstEntry;
				
/*				if(logger.isDebugEnabled()) {
					logger.debug("Low: " + firstEntry + " Up: " + lastEntry + " Pos: " + curEntry);
				}*/
				
				final String curEntryValue = getKeyForIndexEntry(curEntry);
				
				if(curEntryValue.equals(key)) {
					return convertEntryToPosition(curEntry);
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
		
		return -1;
	}

	/**
	 * Get the string key for index entry
	 * @param entry
	 * @return
	 * @throws IOException
	 */
	protected String getKeyForIndexEntry(long entry) throws IOException {
		int position = convertEntryToPosition(entry);
		return sstableReader.decodeOnlyKeyFromTupleAtPosition(position);
	}

	/**
	 * Convert the index entry to index file position
	 * @param entry
	 * @return
	 */
	protected int convertEntryToPosition(long entry) {
		
		// Memory was unmapped
		if(memory == null) {
			return -1;
		}
		
		memory.position((int) ((entry * SSTableConst.INDEX_ENTRY_BYTES) + SSTableConst.MAGIC_BYTES.length));
		int position = memory.getInt();
		return position;
	}

	/**
	 * Get the total number of entries
	 * @return
	 */
	protected int getNumberOfEntries() {
		try {
			if(fileChannel == null) {
				logger.warn("getNumberOfEntries() called on closed sstableindexreader for relation: " + name);
				return 0;
			}
			
			return (int) ((fileChannel.size() - SSTableConst.MAGIC_BYTES.length) / SSTableConst.INDEX_ENTRY_BYTES);
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
	 * Propergate the acquire call to the sstable
	 */
	@Override
	public boolean acquire() {
		
		if(! sstableReader.acquire()) {
			return false;
		}
		
		return super.acquire();
	}
	
	/**
	 * Propergate the release call to the sstable
	 */
	@Override
	public void release() {
		super.release();
		sstableReader.release();
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
					logger.error("Got exception while iterating");
				}
								
				return null;
			}

			@Override
			public void remove() {
				throw new IllegalStateException("Remove is not supported");
			}
		};
	}
}
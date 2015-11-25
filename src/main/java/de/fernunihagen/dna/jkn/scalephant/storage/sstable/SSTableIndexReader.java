package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.IOException;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public class SSTableIndexReader extends AbstractTableReader {
	
	/**
	 * The coresponding sstable reader
	 */
	protected final SSTableReader sstableReader;

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
				
				//System.out.println("Low: " + firstEntry + " Up: " + lastEntry + " Pos: " + curEntry);
				
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
		memory.position((int) ((entry * SSTableConst.INDEX_ENTRY_BYTES) + SSTableConst.MAGIC_BYTES.length));
		int position = memory.getInt();
		return position;
	}

	/**
	 * Get the total number of entries
	 * @return
	 * @throws IOException
	 */
	protected int getNumberOfEntries() throws IOException {
		return (int) ((fileChannel.size() - SSTableConst.MAGIC_BYTES.length) / SSTableConst.INDEX_ENTRY_BYTES);
	}

}
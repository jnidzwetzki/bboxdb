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
			
		/*	long firstEntry = 0;
			long lastEntry = fileInputStream.getChannel().size() - SSTableConst.MAGIC_BYTES.length / SSTableConst.INDEX_ENTRY_BYTES;
			
			long curEntry = (long) ((lastEntry - firstEntry) / 2.0);
			*/
			
			resetPosition();
			
			while(memory.hasRemaining()) {
				int position = memory.getInt();

				final String decodedKey = sstableReader.decodeOnlyKeyFromTupleAtPosition(position);
				
				if(decodedKey.equals(key)) {
					return position;	
				}
				
				if(decodedKey.compareTo(key) > 0) {
					return -1;
				}
				
			}
			
		} catch (IOException e) {
			throw new StorageManagerException("Error while reading index file", e);
		}
		
		return -1;
	}

}
package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public class SSTableIndexReader implements Lifecycle {
	
	/**
	 * The number of the table
	 */
	protected final int tablebumber;
	
	/**
	 * The name of the table
	 */
	protected final String name;
	
	/**
	 * The filename of the table
	 */
	protected final File file;
	
	/**
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(AbstractTableReader.class);
	
	/**
	 * The coresponding sstable reader
	 */
	protected final SSTableReader sstableReader;
	
	/**
	 * The memory region
	 */
	private MappedByteBuffer memory;

	/**
	 * The coresponding fileChanel
	 */
	private FileChannel fileChannel;

	public SSTableIndexReader(final SSTableReader sstableReader) throws StorageManagerException {
		this.name = sstableReader.getName();
		this.directory = sstableReader.getDirectory();
		this.file = constructFileFromReader(sstableReader);
		this.tablebumber = SSTableHelper.extractSequenceFromFilename(name, file.getName());
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
	public long getPositionForTuple(final String key) throws StorageManagerException {
		
		try {
			
		/*	long firstEntry = 0;
			long lastEntry = fileInputStream.getChannel().size() - SSTableConst.MAGIC_BYTES.length / SSTableConst.INDEX_ENTRY_BYTES;
			
			long curEntry = (long) ((lastEntry - firstEntry) / 2.0);
			*/
			
			memory.position(SSTableConst.MAGIC_BYTES.length);
			
			while(memory.hasRemaining()) {
				memory.order(SSTableConst.SSTABLE_BYTE_ORDER);
				long position = memory.getLong();

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

	@Override
	public void init() {
		try {
			fileChannel = new RandomAccessFile(file, "r").getChannel();
			memory = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
		} catch (FileNotFoundException e) {
			logger.error("Error during an IO operation", e);
		} catch (IOException e) {
			logger.error("Error during an IO operation", e);
		}
		
	}

	@Override
	public void shutdown() {
		
		if(fileChannel != null) {
			try {
				fileChannel.close();
				fileChannel = null;
			} catch (IOException e) {
				logger.error("Error during an IO operation", e);
			}
		}
	}

}
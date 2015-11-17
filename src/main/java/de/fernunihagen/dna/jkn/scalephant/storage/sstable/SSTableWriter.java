package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public class SSTableWriter {
	
	/**
	 * The number of the table
	 */
	protected final int tablebumber;
	
	/**
	 * The name of the table
	 */
	protected final String name;
	
	/**
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);
	
	
	public SSTableWriter(final String directory, final String name, final int tablenumber) {
		this.directory = directory;
		this.name = name;
		this.tablebumber = tablenumber;
	}
	
	public void open() throws StorageManagerException {
		final File directoryHandle = new File(directory);
		
		if(! directoryHandle.isDirectory()) {
			final String error = "Directory for SSTables does not exist: " + directory;
			logger.error(error);
			throw new StorageManagerException(error);
		}
		
		final String outputFileName = SSTableManager.getSSTableFilename(directory, name, tablebumber);
		final File outputFile = new File(outputFileName);
		
		logger.info("Opening new SSTable for relation: " + name + " file: " + outputFileName);
	}
	
	public void close() {
		
	}
	
	public void addData(final Memtable memtable) {
	}

}

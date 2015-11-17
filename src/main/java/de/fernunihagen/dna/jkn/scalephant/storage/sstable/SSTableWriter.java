package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public class SSTableWriter implements AutoCloseable {
	
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
	 * File output stream
	 */
	protected FileOutputStream fileOutputStream;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);
	
	
	public SSTableWriter(final String directory, final String name, final int tablenumber) {
		this.directory = directory;
		this.name = name;
		this.tablebumber = tablenumber;
		
		this.fileOutputStream = null;
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

		// Dont overwrite old data
		if(outputFile.exists()) {
			throw new StorageManagerException("Output file alredy exsits: " + outputFileName);
		}
		
		try {
			logger.info("Opening new SSTable for relation: " + name + " file: " + outputFileName);
			fileOutputStream = new FileOutputStream(outputFileName);
			fileOutputStream.write(SSTableConst.MAGIC_BYTES);
		} catch (FileNotFoundException e) {
			throw new StorageManagerException("Unable to open output file", e);
		} catch (IOException e) {
			throw new StorageManagerException("Unable to write into output file", e);
		}
	}
	
	public void close() throws IOException {
		if(fileOutputStream != null) {
			fileOutputStream.close();
			fileOutputStream = null;
		}
	}
	
	public void addData(final Memtable memtable) throws StorageManagerException {
		if(fileOutputStream == null) {
			final String error = "Trying to add a memtable to a non ready SSTable writer";
			logger.error(error);
			throw new StorageManagerException(error);
		}
	}

}

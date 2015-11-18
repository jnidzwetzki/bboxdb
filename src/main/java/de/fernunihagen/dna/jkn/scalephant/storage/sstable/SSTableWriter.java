package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

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
	
	public File open() throws StorageManagerException {
		final String directoryName = SSTableManager.getSSTableDir(directory, name);
		final File directoryHandle = new File(directoryName);
		
		if(! directoryHandle.isDirectory()) {
			final String error = "Directory for SSTable " + name + " does not exist: " + directoryName;
			logger.error(error);
			throw new StorageManagerException(error);
		}
		
		final String outputFileName = SSTableManager.getSSTableFilename(directory, name, tablebumber);
		final File outputFile = new File(outputFileName);

		// Dont overwrite old data
		if(outputFile.exists()) {
			throw new StorageManagerException("Output file already exists: " + outputFileName);
		}
		
		try {
			logger.info("Opening new SSTable for relation: " + name + " file: " + outputFileName);
			fileOutputStream = new FileOutputStream(outputFileName);
			fileOutputStream.write(SSTableConst.MAGIC_BYTES);
			return outputFile;
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
	
	/**
	 * Format of a data record:
	 * 
	 * -----------------------------------------------------------
	 * | Key-Length | Data-Length | Timestamp |   Key   |  Data  |
	 * |   2 Byte   |    8 Byte   |   8 Byte  |         |        |
	 * -----------------------------------------------------------
	 * 
	 * @param tuples
	 * @throws StorageManagerException
	 */
	public void addData(final List<Tuple> tuples) throws StorageManagerException {
		if(fileOutputStream == null) {
			final String error = "Trying to add a memtable to a non ready SSTable writer";
			logger.error(error);
			throw new StorageManagerException(error);
		}
		
		try {
			for(final Tuple tuple : tuples) {
				byte[] keyBytes = tuple.getKey().getBytes();
				byte[] data = tuple.getDataBytes();
				
				final ByteBuffer keyLengthBytes = SSTableHelper.shortToByteBuffer((short) keyBytes.length);
				final ByteBuffer dataLengthBytes = SSTableHelper.longToByteBuffer(data.length);
			    final ByteBuffer timestampBytes = SSTableHelper.longToByteBuffer(tuple.getTimestamp());
			    
			    fileOutputStream.write(keyLengthBytes.array());
				fileOutputStream.write(dataLengthBytes.array());
				fileOutputStream.write(timestampBytes.array());
				fileOutputStream.write(keyBytes);
				fileOutputStream.write(data);
			}
		} catch (IOException e) {
			throw new StorageManagerException("Untable to write memtable to SSTable", e);
		}
	}



}

package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.DeletedTuple;
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
	 * SSTable output stream
	 */
	protected FileOutputStream sstableOutputStream;
	
	/**
	 * SSTable index stream
	 */
	protected FileOutputStream sstableIndexOutputStream;
	
	/**
	 * The SSTable file object
	 */
	protected File sstableFile;
	
	/**
	 * The SSTable index file object
	 */
	protected File sstableIndexFile;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);


	
	public SSTableWriter(final String directory, final String name, final int tablenumber) {
		this.directory = directory;
		this.name = name;
		this.tablebumber = tablenumber;
		
		this.sstableOutputStream = null;
	}
	
	public void open() throws StorageManagerException {
		final String directoryName = SSTableManager.getSSTableDir(directory, name);
		final File directoryHandle = new File(directoryName);
		
		if(! directoryHandle.isDirectory()) {
			final String error = "Directory for SSTable " + name + " does not exist: " + directoryName;
			logger.error(error);
			throw new StorageManagerException(error);
		}
		
		final String sstableOutputFileName = SSTableManager.getSSTableFilename(directory, name, tablebumber);
		sstableFile = new File(sstableOutputFileName);
		
		final String outputIndexFileName = SSTableManager.getSSTableIndexFilename(directory, name, tablebumber);
		sstableIndexFile = new File(outputIndexFileName);
		
		// Don't overwrite old data (table)
		if(sstableFile.exists() || sstableIndexFile.exists()) {
			throw new StorageManagerException("Table file already exists: " + sstableOutputFileName + " / " + sstableIndexFile);
		}
		
		try {
			logger.info("Opening new SSTable for relation: " + name + " file: " + sstableOutputFileName);
			sstableOutputStream = new FileOutputStream(sstableFile);
			sstableOutputStream.write(SSTableConst.MAGIC_BYTES);
			sstableIndexOutputStream = new FileOutputStream(sstableIndexFile);
			sstableIndexOutputStream.write(SSTableConst.MAGIC_BYTES);
		} catch (FileNotFoundException e) {
			throw new StorageManagerException("Unable to open output file", e);
		} catch (IOException e) {
			throw new StorageManagerException("Unable to write into output file", e);
		}
	}
	
	public void close() throws IOException {
		if(sstableOutputStream != null) {
			sstableOutputStream.close();
			sstableOutputStream = null;
		}
		
		if(sstableIndexOutputStream != null) {
			sstableIndexOutputStream.close();
			sstableIndexOutputStream = null;
		}
	}
	
	/**
	 * Format of a data record:
	 * 
	 * ----------------------------------------------------------------------------------
	 * | Key-Length | BBox-Length | Data-Length | Timestamp |   Key   |  BBox  |  Data  |
	 * |   2 Byte   |    4 Byte   |    4 Byte   |   8 Byte  |         |        |        |
	 * ----------------------------------------------------------------------------------
	 * 
	 * @param tuples
	 * @throws StorageManagerException
	 */
	public void addData(final List<Tuple> tuples) throws StorageManagerException {
		if(sstableOutputStream == null) {
			final String error = "Trying to add a memtable to a non ready SSTable writer";
			logger.error(error);
			throw new StorageManagerException(error);
		}
		
		try {
			for(final Tuple tuple : tuples) {
				// Add Tuple to the index
				long tuplePosition = sstableOutputStream.getChannel().position();
			    writeIndexEntry((int) tuplePosition);
			    
			    // Add Tuple to the SSTable file
				writeTupleToFile(tuple);
			}
		} catch (IOException e) {
			throw new StorageManagerException("Untable to write memtable to SSTable", e);
		}
	}

	/**
	 * Write the given tuple into the SSTable
	 * @param tuple
	 * @throws IOException
	 */
	protected void writeTupleToFile(final Tuple tuple) throws IOException {
		final byte[] keyBytes = tuple.getKey().getBytes();
		final ByteBuffer keyLengthBytes = SSTableHelper.shortToByteBuffer((short) keyBytes.length);

		final byte[] boundingBoxBytes = tuple.getBoundingBoxBytes();
		final byte[] data = tuple.getDataBytes();
		
		final ByteBuffer boxLengthBytes = SSTableHelper.intToByteBuffer(boundingBoxBytes.length);
		final ByteBuffer dataLengthBytes = SSTableHelper.intToByteBuffer(data.length);
	    final ByteBuffer timestampBytes = SSTableHelper.longToByteBuffer(tuple.getTimestamp());
	    
	    sstableOutputStream.write(keyLengthBytes.array());
		sstableOutputStream.write(boxLengthBytes.array());
		sstableOutputStream.write(dataLengthBytes.array());
		sstableOutputStream.write(timestampBytes.array());
		sstableOutputStream.write(keyBytes);
		sstableOutputStream.write(boundingBoxBytes);
		sstableOutputStream.write(data);
	}

	/** 
	 * Append an entry to the index file.
	 * 
	 * Format of the index file:
	 * 
	 * -------------------------------------------------
	 * | Tuple-Position | Tuple-Position |  .........  |
 	 * |     4 Byte     |     4 Byte     |  .........  |
	 * -------------------------------------------------
	 * 
	 * @param keyLengthBytes
	 * @param keyPosition
	 * @throws IOException
	 */
	protected void writeIndexEntry(int tuplePosition) throws IOException {
		final ByteBuffer tuplePositionBytes = SSTableHelper.intToByteBuffer(tuplePosition);
		sstableIndexOutputStream.write(tuplePositionBytes.array());
	}

	/**
	 * Get the sstable output file
	 * @return
	 */
	public File getSstableFile() {
		return sstableFile;
	}

	/**
	 * Get the sstable index output file
	 * @return
	 */
	public File getSstableIndexFile() {
		return sstableIndexFile;
	}
	
}

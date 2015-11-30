package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public abstract class AbstractTableReader implements Lifecycle {

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
	 * The memory region
	 */
	protected MappedByteBuffer memory;

	/**
	 * The coresponding fileChanel
	 */
	protected FileChannel fileChannel;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(AbstractTableReader.class);
	
	
	public AbstractTableReader(final String name, final String directory, final File file) throws StorageManagerException {
		this.name = name;
		this.directory = directory;
		this.file = file;
		this.tablebumber = SSTableHelper.extractSequenceFromFilename(name, file.getName());
	}
	
	/**
	 * Get the sequence number of the SSTable
	 * 
	 * @return
	 */
	public int getTablebumber() {
		return tablebumber;
	}


	/**
	 * Open a stored SSTable and read the magic bytes
	 * 
	 * @return a InputStream or null
	 * @throws StorageManagerException
	 */
	protected void validateFile() throws StorageManagerException {
		
		// Validate file - read the magic from the beginning
		final byte[] magicBytes = new byte[SSTableConst.MAGIC_BYTES.length];
		memory.get(magicBytes, 0, SSTableConst.MAGIC_BYTES.length);

		if(! Arrays.equals(magicBytes, SSTableConst.MAGIC_BYTES)) {
			throw new StorageManagerException("File " + file + " does not contain the magic bytes");
		}

	}
	
	/**
	 * Reset the position to the first element
	 */
	protected void resetPosition() {
		memory.position(SSTableConst.MAGIC_BYTES.length);
	}

	@Override
	public void init() {
		try {
			fileChannel = new RandomAccessFile(file, "r").getChannel();
			memory = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
			memory.order(SSTableConst.SSTABLE_BYTE_ORDER);
			validateFile();
		} catch (FileNotFoundException e) {
			logger.error("Error during an IO operation", e);
		} catch (IOException e) {
			logger.error("Error during an IO operation", e);
		} catch (StorageManagerException e) {
			logger.error("Error during an IO operation", e);
		}
		
	}

	@Override
	public void shutdown() {
		
		memory = null;
		
		if(fileChannel != null) {
			try {
				fileChannel.close();
				fileChannel = null;
			} catch (IOException e) {
				logger.error("Error during an IO operation", e);
			}
		}
	}

	/**
	 * Delete the unterlaying file
	 */
	public boolean delete() {
		if(file != null) {
			logger.info("Delete file: " + file);
			file.delete();
			return true;
		}
		
		return false;
	}
	
	/**
	 * Is the reader ready?
	 */
	protected boolean isReady() {
		return memory != null;
	}

	/**
	 * Get the name
	 * @return the file handle
	 */
	public String getName() {
		return name;
	}

	/**
	 * Get the file handle
	 * @return
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Get the directory
	 * @return
	 */
	public String getDirectory() {
		return directory;
	}

}

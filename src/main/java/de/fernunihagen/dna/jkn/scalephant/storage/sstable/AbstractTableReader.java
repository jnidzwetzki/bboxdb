package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
	 * The reader
	 */
	protected InputStream reader;
	/**
	 * The to reader coresponsing fileInputStream
	 */
	protected FileInputStream fileInputStream;
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(AbstractTableReader.class);

	public AbstractTableReader(final String name, final String directory, final File file) throws StorageManagerException {
		this.name = name;
		this.directory = directory;
		this.file = file;
		this.tablebumber = extractSequenceFromFilename(file.getName());
		this.reader = null;
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
	 * Set the position of the reader at the position of the first tuple
	 * 
	 * @throws IOException
	 */
	protected void resetFileReaderPosition() throws IOException {
		fileInputStream.getChannel().position(SSTableConst.MAGIC_BYTES.length);
		createNewReaderBuffer();
	}

	/**
	 * Open a stored SSTable and read the magic bytes
	 * 
	 * @return a InputStream or null
	 * @throws StorageManagerException
	 */
	protected void openAndValidateFile() throws StorageManagerException {
		
		try {
			fileInputStream = new FileInputStream(file);
			
			createNewReaderBuffer();
			
			// Validate file - read the magic from the beginning
			final byte[] magicBytes = new byte[SSTableConst.MAGIC_BYTES.length];
			reader.read(magicBytes, 0, SSTableConst.MAGIC_BYTES.length);
	
			if(! Arrays.equals(magicBytes, SSTableConst.MAGIC_BYTES)) {
				throw new StorageManagerException("File " + file + " does not contain the magic bytes");
			}
			
		} catch (FileNotFoundException e) {
			final String error = "Unable to open SSTable: " + file;
			logger.error(error);
			throw new StorageManagerException(error, e);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

	/**
	 * Create a new reader buffer. This is needed after changing the position 
	 * of the underlying stream
	 */
	protected void createNewReaderBuffer() {
		reader = new BufferedInputStream(fileInputStream);
	}

	/**
	 * Extract the sequence Number from a given filename
	 * 
	 * @param filename
	 * @return the sequence number
	 * @throws StorageManagerException 
	 */
	protected int extractSequenceFromFilename(final String filename)
			throws StorageManagerException {
				try {
					final String sequence = filename
						.replace(SSTableConst.SST_FILE_PREFIX + name + "_", "")
						.replace(SSTableConst.SST_FILE_SUFFIX, "");
				
					return Integer.parseInt(sequence);
				
				} catch (NumberFormatException e) {
					String error = "Unable to parse sequence number: " + filename;
					logger.warn(error);
					throw new StorageManagerException(error, e);
				}
			}

	/**
	 * Init the reader
	 */
	@Override
	public void init() {
		try {
			openAndValidateFile();
		} catch (StorageManagerException e) {
			logger.error("Unable to init reader: ", e);
		}
	}

	/** 
	 * Shutdown the reader
	 */
	@Override
	public void shutdown() {
		if(reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				logger.error("Unable to close reader: ", e);
			}
		}
	}

	/**
	 * Is the reader ready?
	 */
	protected boolean isReady() {
		return reader != null;
	}

}

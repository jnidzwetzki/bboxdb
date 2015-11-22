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

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class SSTableReader {
	
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
	private final static Logger logger = LoggerFactory.getLogger(SSTableReader.class);
	
	public SSTableReader(final String name, final String directory, final File file) throws StorageManagerException {
		this.name = name;
		this.directory = directory;
		this.file = file;
		this.tablebumber = extractSequenceFromFilename(file.getName());
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
	 * Scan the whole SSTable for the Tuple
	 * @param key
	 * @return the tuple or null
	 * @throws StorageManagerException 
	 */
	public Tuple getTuple(final String key) throws StorageManagerException {
		logger.info("Search in table: " + tablebumber + " for " + key);
		

		try (final InputStream reader = openAndValidateFile()) {
			
			while(reader.available() > 0) {
				
				final Tuple tuple = decodeTuple(reader);
				
				if(tuple.getKey().equals(key)) {
					return tuple;
				}
			}
		
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
		
		return null;
	}

	/**
	 * Decode the tuple at the current reader position
	 * 
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public Tuple decodeTuple(final InputStream reader) throws IOException {
		byte[] keyLengthBytes = new byte[SSTableHelper.SHORT_BYTES];
		byte[] boxLengthBytes = new byte[SSTableHelper.INT_BYTES];
		byte[] dataLengthBytes = new byte[SSTableHelper.INT_BYTES];
		byte[] timestampBytes = new byte[SSTableHelper.LONG_BYTES];
		
		reader.read(keyLengthBytes, 0, keyLengthBytes.length);
		reader.read(boxLengthBytes, 0, boxLengthBytes.length);
		reader.read(dataLengthBytes, 0, dataLengthBytes.length);
		reader.read(timestampBytes, 0, timestampBytes.length);
		
		final short keyLength = SSTableHelper.readShortFromByteBuffer(keyLengthBytes);
		final int boxLength = SSTableHelper.readIntFromByteBuffer(boxLengthBytes);
		final int dataLength = SSTableHelper.readIntFromByteBuffer(dataLengthBytes);
		final long timestamp = SSTableHelper.readLongFromByteBuffer(timestampBytes);
		
		byte[] keyBytes = new byte[keyLength];
		reader.read(keyBytes, 0, keyBytes.length);
		
		byte[] boxBytes = new byte[boxLength];
		reader.read(boxBytes, 0, boxBytes.length);
		
		byte[] dataBytes = new byte[dataLength];
		reader.read(dataBytes, 0, dataBytes.length);				
		
		final long[] longArray = SSTableHelper.readLongArrayFromByteBuffer(boxBytes);
		final BoundingBox boundingBox = new BoundingBox(longArray);
		
		final String keyString = new String(keyBytes);
		
		return new Tuple(keyString, boundingBox, dataBytes, timestamp);
	}
	
	/**
	 * Open a stored SSTable and read the magic bytes
	 * 
	 * @return a InputStream or null
	 * @throws StorageManagerException
	 */
	protected InputStream openAndValidateFile() throws StorageManagerException {
		
		try {
			final InputStream reader = new BufferedInputStream(new FileInputStream(file));
			
			// Validate file - read the magic from the beginning
			final byte[] magicBytes = new byte[SSTableConst.MAGIC_BYTES.length];
			reader.read(magicBytes, 0, SSTableConst.MAGIC_BYTES.length);

			if(! Arrays.equals(magicBytes, SSTableConst.MAGIC_BYTES)) {
				throw new StorageManagerException("File " + file + " does not contain the magic bytes");
			}
			
			return reader;
		} catch (FileNotFoundException e) {
			final String error = "Unable to open SSTable: " + file;
			logger.error(error);
			throw new StorageManagerException(error, e);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}
	
	/**
	 * Extract the sequence Number from a given filename
	 * 
	 * @param filename
	 * @return the sequence number
	 * @throws StorageManagerException 
	 */
	protected int extractSequenceFromFilename(final String filename) throws StorageManagerException {
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

	@Override
	public String toString() {
		return "SSTableReader [tablebumber=" + tablebumber + ", name=" + name
				+ ", directory=" + directory + "]";
	}
}

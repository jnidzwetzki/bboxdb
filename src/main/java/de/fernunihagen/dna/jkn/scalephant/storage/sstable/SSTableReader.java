package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

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
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableReader.class);
	
	public SSTableReader(final String name, final String directory, final String filename) throws StorageManagerException {
		this.name = name;
		this.directory = directory;
		this.tablebumber = extractSequenceFromFilename(filename);
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
	 * Extract the sequence Number from a given filename
	 * 
	 * @param filename
	 * @return the sequence number
	 * @throws StorageManagerException 
	 */
	protected int extractSequenceFromFilename(final String filename) throws StorageManagerException {
		try {
			final String sequence = filename
				.replace(SSTableConst.FILE_PREFIX + name + "_", "")
				.replace(SSTableConst.FILE_SUFFIX, "");
		
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

package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Lifecycle;

public class SSTableManager implements Lifecycle {
	
	/**
	 * The name of the table
	 */
	protected final String name;
	
	/**
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableManager.class);

	public SSTableManager(final String name, final String directory) {
		super();
		this.name = name;
		this.directory = directory;
	}

	@Override
	public void init() {
		logger.info("Init a new instance for the table: " + name);
	}

	@Override
	public void shutdown() {
		logger.info("Shuting down the instance for table: " + name);
	}
	
	
	protected static String getSSTableFilename(final String directoy, final String name, int tablebumber) {
		return directoy 
				+ File.separator 
				+ SSTableConst.FILE_PREFIX 
				+ name 
				+ "_" 
				+ tablebumber 
				+ SSTableConst.FILE_SUFFIX;
	}

}

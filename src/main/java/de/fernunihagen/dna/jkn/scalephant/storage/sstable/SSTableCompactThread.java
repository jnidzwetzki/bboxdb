package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableCompactThread implements Runnable {

	
	/**
	 * The corresponding SSTable manager
	 */
	protected final SSTableManager sstableManager;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCompactThread.class);


	public SSTableCompactThread(final SSTableManager ssTableManager) {
		this.sstableManager = ssTableManager;
	}

	/**
	 * Compact our SSTables
	 * 
	 */
	@Override
	public void run() {
		logger.info("Starting new compact thread for: " + sstableManager.getName());
	}

	
}

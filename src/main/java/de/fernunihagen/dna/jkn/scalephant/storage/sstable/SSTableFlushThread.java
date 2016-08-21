package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.jkn.scalephant.util.Stoppable;

class SSTableFlushThread implements Runnable, Stoppable {

	/**
	 * The reference to the sstable Manager
	 */
	protected final SSTableManager sstableManager;
	
	/**
	 * The data directory
	 */
	protected String dataDirectory;
	
	/**
	 * The unflushed memtables
	 */
	protected final List<Memtable> unflushedMemtables;

	/**
	 * The run variable
	 */
	protected volatile boolean run;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableFlushThread.class);

	/**
	 * @param ssTableManager
	 */
	SSTableFlushThread(final SSTableManager sstableManager) {
		this.sstableManager = sstableManager;
		this.unflushedMemtables = sstableManager.getUnflushedMemtables();
		this.dataDirectory = sstableManager.getScalephantConfiguration().getDataDirectory();
		this.run = true;
	}

	/**
	 * Watch the unflushedMemtables list for unflushed memtables and flush 
	 * them onto disk
	 */
	@Override
	public void run() {
		while(run) {
			while(unflushedMemtables.isEmpty()) {
				try {					
					synchronized (unflushedMemtables) {
						unflushedMemtables.wait();
					}
				} catch (InterruptedException e) {
					logger.info("Memtable flush thread has stopped: " + sstableManager.getSSTableName());
					return;
				}
			}
			
			flushAllMemtablesToDisk();
		}
		
		logger.info("Memtable flush thread has stopped: " + sstableManager.getSSTableName());
	}
	
	/**
	 * Stop the memtable flush thread
	 */
	public void stop() {
		logger.info("Stopping memtable flush thread for: " + sstableManager.getSSTableName());
		run = false;
	}

	/**
	 * Flush all pending memtables to disk
	 * 
	 */
	protected void flushAllMemtablesToDisk() {
		while(! unflushedMemtables.isEmpty()) {
			final Memtable memtable = unflushedMemtables.get(0);

			try {
				final int tableNumber = writeMemtable(memtable);
				final SSTableFacade facade = new SSTableFacade(dataDirectory, sstableManager.getSSTableName(), tableNumber);
				facade.init();
				sstableManager.sstableFacades.add(facade);
			} catch (Exception e) {
				
				sstableManager.storageState.setReady(false);
				
				if(Thread.currentThread().isInterrupted()) {
					logger.warn("Got Exception while flushing memtable, but thread was interrupted. Ignoring exception.");
				} else {
					logger.warn("Exception while flushing memtable: " + sstableManager.getSSTableName(), e);
				}
			}
	
			unflushedMemtables.remove(memtable);
		}
		
		synchronized (unflushedMemtables) {
			unflushedMemtables.notifyAll();
		}
	}
		
	/**
	 * Write a memtable to disk and return the file handle of the table
	 * 
	 * @param memtable
	 * @return
	 * @throws Exception 
	 */
	protected int writeMemtable(final Memtable memtable) throws Exception {
		int tableNumber = sstableManager.increaseTableNumber();
		logger.info("Writing new memtable: " + tableNumber);
		
		try(final SSTableWriter ssTableWriter = new SSTableWriter(dataDirectory, sstableManager.getSSTableName(), tableNumber)) {
			ssTableWriter.open();
			ssTableWriter.getSstableFile();
			ssTableWriter.addData(memtable.getSortedTupleList());
			return tableNumber;
		} catch (Exception e) {
			throw e;
		} 
	}
}
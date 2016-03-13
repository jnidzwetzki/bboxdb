package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

class SSTableFlushThread implements Runnable {

	/**
	 * The reference to the sstable Manager
	 */
	private final SSTableManager sstableManager;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableFlushThread.class);

	/**
	 * @param ssTableManager
	 */
	SSTableFlushThread(SSTableManager sstableManager) {
		this.sstableManager = sstableManager;
	}

	/**
	 * Watch the unflushedMemtables list for unflushed memtables and flush 
	 * them onto disk
	 */
	@Override
	public void run() {
		while(sstableManager.isReady()) {
			while(sstableManager.unflushedMemtables.isEmpty()) {
				try {					
					synchronized (sstableManager.unflushedMemtables) {
						sstableManager.unflushedMemtables.wait();
					}
				} catch (InterruptedException e) {
					logger.info("Memtable flush thread has stopped: " + sstableManager.getName());
					return;
				}
			}
			
			flushAllMemtablesToDisk();
		}
		
		logger.info("Memtable flush thread has stopped: " + sstableManager.getName());
	}

	/**
	 * Flush all pending memtables to disk
	 * 
	 */
	protected void flushAllMemtablesToDisk() {
		while(! sstableManager.unflushedMemtables.isEmpty()) {
			final Memtable memtable = sstableManager.unflushedMemtables.get(0);
			final int tableNumber = writeMemtable(memtable);
			
			if(tableNumber != -1) {
				try {
					final SSTableReader reader = new SSTableReader(getStorageDataDir(), sstableManager.getName(), tableNumber);
					sstableManager.sstableReader.add(reader);
				} catch (StorageManagerException e) {
					logger.error("Exception while creating SSTable reader", e);
				}
			}
			
			sstableManager.unflushedMemtables.remove(memtable);
		}
	}

	/**
	 * Get the root directory for storage
	 * @return
	 */
	protected String getStorageDataDir() {
		return sstableManager.getStorageConfiguration().getDataDirectory();
	}
		
	/**
	 * Write a memtable to disk and return the Filehandle of the table
	 * 
	 * @param memtable
	 * @return
	 */
	protected int writeMemtable(final Memtable memtable) {
		int tableNumber = sstableManager.increaseTableNumber();
		logger.info("Writing new memtable: " + tableNumber);
		
		try(final SSTableWriter ssTableWriter = new SSTableWriter(getStorageDataDir(), sstableManager.getName(), tableNumber)) {
			ssTableWriter.open();
			ssTableWriter.getSstableFile();
			ssTableWriter.addData(memtable.getSortedTupleList());
			return tableNumber;
		} catch (Exception e) {
			logger.info("Exception while write memtable: " + sstableManager.getName() + " / " + tableNumber, e);
			sstableManager.storageState.setReady(false);
		} 
		
		return -1;
	}
}
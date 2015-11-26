package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

class SSTableFlusher implements Runnable {

	/**
	 * The reference to the sstable Manager
	 */
	private final SSTableManager sstableManager;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableFlusher.class);

	/**
	 * @param ssTableManager
	 */
	SSTableFlusher(SSTableManager sstableManager) {
		this.sstableManager = sstableManager;
	}

	/**
	 * Watch the unflushedMemtables list for unflushed memtables and flush 
	 * them onto disk
	 */
	@Override
	public void run() {
		while(sstableManager.ready) {
			while(sstableManager.unflushedMemtables.isEmpty()) {
				try {					
					synchronized (sstableManager.unflushedMemtables) {
						sstableManager.unflushedMemtables.wait();
					}
				} catch (InterruptedException e) {
					logger.info("Memtable flush thread has stopped: " + sstableManager.name);
					return;
				}
			}
			
			flushAllMemtablesToDisk();
		}
		
		logger.info("Memtable flush thread has stopped: " + sstableManager.name);
	}

	/**
	 * Flush all pending memtables to disk
	 * 
	 */
	protected void flushAllMemtablesToDisk() {
		while(! sstableManager.unflushedMemtables.isEmpty()) {
			final Memtable memtable = sstableManager.unflushedMemtables.get(0);
			final File sstableFile = writeMemtable(memtable);
			
			if(sstableFile != null) {
				try {
					final SSTableReader reader = new SSTableReader(sstableManager.name, sstableManager.directory, sstableFile);
					sstableManager.tableReader.add(reader);
				} catch (StorageManagerException e) {
					logger.error("Exception while creating SSTable reader", e);
				}
			}
			
			final Memtable removedTable = sstableManager.unflushedMemtables.remove(0);

			if(memtable != removedTable) {
				logger.error("Get other table than removed!");
			}

		}
	}
		
	/**
	 * Write a memtable to disk and return the Filehandle of the table
	 * 
	 * @param memtable
	 * @return
	 */
	protected File writeMemtable(final Memtable memtable) {
		logger.info("Writing new memtable: " + sstableManager.tableNumber);
		
		try(final SSTableWriter ssTableWriter = new SSTableWriter(sstableManager.directory, sstableManager.name, sstableManager.tableNumber)) {
			ssTableWriter.open();
			final File filehandle = ssTableWriter.getSstableFile();
			ssTableWriter.addData(memtable.getSortedTupleList());
			return filehandle;
		} catch (Exception e) {
			logger.info("Exception while write memtable: " + sstableManager.name + " / " + sstableManager.tableNumber, e);
			sstableManager.storageState.setReady(false);
		} finally {
			sstableManager.tableNumber++;
		}
		
		return null;
	}
}
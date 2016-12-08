/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package de.fernunihagen.dna.scalephant.storage.sstable;

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.storage.Memtable;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.util.Stoppable;

class MemtableFlushThread implements Runnable, Stoppable {

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
	protected final Queue<Memtable> unflushedMemtables;

	/**
	 * The run variable
	 */
	protected volatile boolean run;

	/**
	 * The name of the thread
	 */
	protected final String threadname;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory
			.getLogger(MemtableFlushThread.class);

	/**
	 * @param ssTableManager
	 */
	MemtableFlushThread(final SSTableManager ssTableManager) {
		this.sstableManager = ssTableManager;
		this.unflushedMemtables = ssTableManager.getTupleStoreInstances()
				.getMemtablesToFlush();
		this.dataDirectory = sstableManager.getScalephantConfiguration()
				.getDataDirectory();
		this.threadname = sstableManager.getSSTableName().getFullname();
		this.run = true;
	}

	/**
	 * Watch the unflushedMemtables list for unflushed memtables and flush them
	 * onto disk
	 */
	@Override
	public void run() {
		logger.info("Memtable flush thread has started: {} ", threadname);

		try {
			executeThread();
		} catch (Throwable e) {
			logger.error("Got uncought exception", e);
		}

		logger.info("Memtable flush thread has stopped: {} ", threadname);
	}

	/**
	 * Start the flush thread
	 */
	protected void executeThread() {
		while (run) {
			while (unflushedMemtables.isEmpty()) {
				try {
					synchronized (unflushedMemtables) {
						unflushedMemtables.wait();
					}
				} catch (InterruptedException e) {
					logger.info("Memtable flush thread has stopped: {}",
							threadname);
					Thread.currentThread().interrupt();
					return;
				}
			}

			flushAllMemtablesToDisk();
		}
	}

	/**
	 * Stop the memtable flush thread
	 */
	public void stop() {
		logger.info("Stopping memtable flush thread for: " + threadname);
		run = false;
	}

	/**
	 * Flush all pending memtables to disk
	 * 
	 */
	protected void flushAllMemtablesToDisk() {
		while (!unflushedMemtables.isEmpty()) {
			final Memtable memtable = unflushedMemtables.remove();

			if (memtable == null) {
				continue;
			}

			if (memtable.isEmpty()) {
				continue;
			}

			try {
				final int tableNumber = writeMemtable(memtable);
				final SSTableFacade facade = new SSTableFacade(dataDirectory,
						sstableManager.getSSTableName(), tableNumber);
				facade.init();
				sstableManager.getTupleStoreInstances()
						.replaceMemtableWithSSTable(memtable, facade);
				
				logger.debug("Replacing memtable {} with sstable {}", memtable, facade);
				
			} catch (Exception e) {

				sstableManager.storageState.setReady(false);

				if (Thread.currentThread().isInterrupted()) {
					logger.warn("Got Exception while flushing memtable, but thread was interrupted. Ignoring exception.");
				} else {
					logger.warn("Exception while flushing memtable: "
							+ threadname, e);
				}
			}

			unflushedMemtables.remove(memtable);
			memtable.deleteOnClose();
			memtable.release();
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
		logger.info("Writing new memtable number: {} ", tableNumber);

		try (final SSTableWriter ssTableWriter = new SSTableWriter(
				dataDirectory, sstableManager.getSSTableName(), tableNumber,
				memtable.getMaxEntries())) {

			ssTableWriter.open();
			ssTableWriter.getSstableFile();
			ssTableWriter.addData(memtable.getSortedTupleList());
			return tableNumber;
		} catch (Exception e) {
			throw e;
		}
	}
}
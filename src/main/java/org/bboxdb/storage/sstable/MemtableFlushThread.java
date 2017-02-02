/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.storage.sstable;

import java.util.Queue;

import org.bboxdb.storage.Memtable;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.util.ExceptionSafeThread;
import org.bboxdb.util.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MemtableFlushThread extends ExceptionSafeThread implements Stoppable {

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
	public MemtableFlushThread(final SSTableManager ssTableManager) {
		
		this.sstableManager = ssTableManager;
		this.unflushedMemtables = ssTableManager.getTupleStoreInstances()
				.getMemtablesToFlush();
		this.dataDirectory = sstableManager.getScalephantConfiguration()
				.getDataDirectory();
		this.threadname = sstableManager.getSSTableName().getFullname();
		
		this.run = true;
	}

	@Override
	protected void beginHook() {
		logger.info("Memtable flush thread has started: {} ", threadname);
	}
	
	@Override
	protected void endHook() {
		logger.info("Memtable flush thread has stopped: {} ", threadname);
	}
	
	/**
	 * Start the flush thread
	 */
	@Override
	protected void runThread() {
		while (run) {
			final Memtable memtable = getNextUnflushedMemtable();
			
			if(memtable == null) {
				logger.debug("Got null memtable, stopping thread");
				return;
			}

			flushMemtableToDisk(memtable);
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
	 * Get the next unflushed memtable
	 * @return
	 */
	protected Memtable getNextUnflushedMemtable() {
		
		synchronized (unflushedMemtables) {
			
			while (unflushedMemtables.isEmpty()) {
				try {
					unflushedMemtables.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					run = false;
					return null;
				}
			}
			
			final Memtable memtable = unflushedMemtables.remove();
			unflushedMemtables.notifyAll();
			return memtable;
		}
	}

	/**
	 * Flush a memtable to disk
	 * @param memtable
	 * 
	 */
	protected void flushMemtableToDisk(final Memtable memtable) {
		
		if (memtable == null || memtable.isEmpty()) {
			return;
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
			handleExceptionDuringFlush(e);
		}

		memtable.deleteOnClose();
		memtable.release();
	}

	/**
	 * Handle the exception during memtable flush
	 * @param e
	 */
	protected void handleExceptionDuringFlush(Exception e) {
		sstableManager.storageState.setReady(false);

		if (Thread.currentThread().isInterrupted()) {
			logger.debug("Got Exception while flushing memtable, but thread was interrupted. "
					+ "Ignoring exception.");
			Thread.currentThread().interrupt();
		} else {
			logger.warn("Exception while flushing memtable: "
					+ threadname, e);
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
		
		final int tableNumber = sstableManager.increaseTableNumber();
		
		logger.info("Writing new memtable number: {} with {} entries and a size of {} KB", 
				tableNumber, memtable.getTotalEntries(), memtable.getSize() / 1024);

		try (final SSTableWriter ssTableWriter = new SSTableWriter(
				dataDirectory, sstableManager.getSSTableName(), tableNumber,
				memtable.getMaxEntries())) {

			ssTableWriter.open();
			ssTableWriter.addData(memtable.getSortedTupleList());
			return tableNumber;
		} catch (Exception e) {
			throw e;
		}
	}
}
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

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bboxdb.storage.Memtable;
import org.bboxdb.storage.SSTableFlushCallback;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.util.ExceptionSafeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MemtableFlushThread extends ExceptionSafeThread {

	/**
	 * The reference to the sstable Manager
	 */
	protected final SSTableManager sstableManager;

	/**
	 * The unflushed memtables
	 */
	protected final BlockingQueue<Memtable> unflushedMemtables;

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
		this.threadname = sstableManager.getSSTableName().getFullname();		
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
		while (! Thread.currentThread().isInterrupted()) {
			try {
				final Memtable memtable = unflushedMemtables.take();
				
				if(memtable == null) {
					logger.debug("Got null memtable, stopping thread");
					break;
				}

				flushMemtableToDisk(memtable);
				
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		
		logger.info("Stopping memtable flush thread for: " + threadname);
	}
	
	/**
	 * Flush a memtable to disk
	 * @param memtable
	 * 
	 */
	protected void flushMemtableToDisk(final Memtable memtable) {
		
		if(memtable == null) {
			return;
		}

		try {
			SSTableFacade facade = null;
			
			// Don't write empty memtables to disk
			if (! memtable.isEmpty()) {
				final SSTableName sstableName = sstableManager.getSSTableName();
				final String dataDirectory = StorageRegistry.getInstance().getStorageDirForSSTable(sstableName);
				final int tableNumber = writeMemtable(dataDirectory, memtable);
				facade = new SSTableFacade(dataDirectory, sstableName, tableNumber);
				facade.init();
			}
			
			sstableManager.getTupleStoreInstances()
					.replaceMemtableWithSSTable(memtable, facade);
						
			sendCallbacks(memtable);	
			
			memtable.deleteOnClose();
			memtable.release();
		} catch (Exception e) {
			handleExceptionDuringFlush(e);
		}
	}

	/**
	 * Send all callbacks for a memtable flush
	 * @param memtable
	 */
	protected void sendCallbacks(final Memtable memtable) {
		final long timestamp = memtable.getCreatedTimestamp();
		final List<SSTableFlushCallback> callbacks = StorageRegistry.getInstance().getSSTableFlushCallbacks();
		
		for(final SSTableFlushCallback callback : callbacks) {
			try {
				callback.flushCallback(sstableManager.getSSTableName(), timestamp);
			} catch(Exception e) {
				logger.error("Got exception while executing callback", e);
			}
		}
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
	 * @param dataDirectory 
	 * 
	 * @param memtable
	 * @return
	 * @throws Exception
	 */
	protected int writeMemtable(final String dataDirectory, final Memtable memtable) throws Exception {
		
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
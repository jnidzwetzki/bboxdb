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
package org.bboxdb.storage.memtable;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bboxdb.storage.SSTableFlushCallback;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.registry.MemtableAndSSTableManager;
import org.bboxdb.storage.registry.Storage;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.storage.sstable.SSTableManagerState;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.util.FileSizeHelper;
import org.bboxdb.util.concurrent.ExceptionSafeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemtableWriterThread extends ExceptionSafeThread {

	/**
	 * The unflushed memtables
	 */
	protected final BlockingQueue<MemtableAndSSTableManager> flushQueue;

	/**
	 * The basedir
	 */
	protected final File basedir;
	
	/**
	 * The storage
	 */
	protected Storage storage;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory
			.getLogger(MemtableWriterThread.class);

	/**
	 * @param ssTableManager
	 */
	public MemtableWriterThread(final Storage storage, final File basedir) {
		this.storage = storage;
		this.flushQueue = storage.getMemtablesToFlush();
		this.basedir = basedir;	
	}

	@Override
	protected void beginHook() {
		logger.info("Memtable writer thread has started");
	}
	
	@Override
	protected void endHook() {
		logger.info("Memtable writer thread has stopped");
	}
	
	/**
	 * Start the flush thread
	 */
	@Override
	protected void runThread() {
		while (! Thread.currentThread().isInterrupted()) {
			try {
				final MemtableAndSSTableManager memtableAndSSTableManager = flushQueue.take();
				final Memtable memtable = memtableAndSSTableManager.getMemtable();
				final SSTableManager sstableManager = memtableAndSSTableManager.getSsTableManager();
				flushMemtableToDisk(memtable, sstableManager);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		
		logger.info("Memtable flush thread has ended");
	}
	
	/**
	 * Flush a memtable to disk
	 * @param memtable
	 * @param sstableManager 
	 * 
	 */
	protected void flushMemtableToDisk(final Memtable memtable, final SSTableManager sstableManager) {
		
		if(memtable == null) {
			return;
		}

		SSTableFacade facade = null;

		try {			
			// Don't write empty memtables to disk
			if (! memtable.isEmpty()) {
				final SSTableName sstableName = sstableManager.getSSTableName();
				final String dataDirectory = basedir.getAbsolutePath();
				final int tableNumber = writeMemtable(dataDirectory, memtable, sstableManager);
				
				final int sstableKeyCacheEntries = storage.getStorageRegistry().getConfiguration()
						.getSstableKeyCacheEntries();
				
				facade = new SSTableFacade(dataDirectory, sstableName, tableNumber, sstableKeyCacheEntries);
				facade.init();
			}
			
			sstableManager.replaceMemtableWithSSTable(memtable, facade);
						
			sendCallbacks(memtable, sstableManager);	
			
			memtable.deleteOnClose();
			memtable.release();
		}  catch (Exception e) {
			deleteWrittenFacade(facade);

			if(sstableManager.getSstableManagerState() == SSTableManagerState.READ_ONLY) {
				logger.debug("Rejected memtable write:", e);
				return;
			}
			
			if (Thread.currentThread().isInterrupted()) {
				logger.debug("Got Exception while flushing memtable, but thread was interrupted. "
						+ "Ignoring exception.");
				Thread.currentThread().interrupt();
				return;
				
			} 
			
			logger.error("Exception while flushing memtable", e);
		}
	}

	/**
	 * Delete the written facade
	 * @param facade
	 */
	protected void deleteWrittenFacade(final SSTableFacade facade) {
		if(facade != null) {
			facade.deleteOnClose();
		}
	}

	/**
	 * Send all callbacks for a memtable flush
	 * @param memtable
	 * @param sstableManager 
	 */
	protected void sendCallbacks(final Memtable memtable, SSTableManager sstableManager) {
		final long timestamp = memtable.getCreatedTimestamp();
		final List<SSTableFlushCallback> callbacks = storage.getStorageRegistry().getSSTableFlushCallbacks();
		
		for(final SSTableFlushCallback callback : callbacks) {
			try {
				callback.flushCallback(sstableManager.getSSTableName(), timestamp);
			} catch(Exception e) {
				logger.error("Got exception while executing callback", e);
			}
		}
	}

	/**
	 * Write a memtable to disk and return the file handle of the table
	 * @param dataDirectory 
	 * 
	 * @param memtable
	 * @param sstableManager 
	 * @return
	 * @throws Exception
	 */
	protected int writeMemtable(final String dataDirectory, final Memtable memtable, 
			final SSTableManager sstableManager) throws Exception {
		
		final int tableNumber = sstableManager.increaseTableNumber();
		
		logger.info("Writing memtable number {} with {} entries and a size of {}", 
				tableNumber, 
				memtable.getTotalEntries(), 
				FileSizeHelper.readableFileSize(memtable.getSize()));

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
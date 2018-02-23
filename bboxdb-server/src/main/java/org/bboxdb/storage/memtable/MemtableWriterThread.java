/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
import java.util.function.BiConsumer;

import org.bboxdb.commons.FileSizeHelper;
import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.storage.entity.MemtableAndTupleStoreManagerPair;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemtableWriterThread extends ExceptionSafeRunnable {

	/**
	 * The basedir
	 */
	protected final File basedir;
	
	/**
	 * The storage
	 */
	protected DiskStorage storage;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MemtableWriterThread.class);

	/**
	 * @param ssTableManager
	 */
	public MemtableWriterThread(final DiskStorage storage, final File basedir) {
		this.storage = storage;
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
				final MemtableAndTupleStoreManagerPair memtableAndSSTableManager = storage.takeNextUnflushedMemtable();
				final Memtable memtable = memtableAndSSTableManager.getMemtable();
				final TupleStoreManager sstableManager = memtableAndSSTableManager.getTupleStoreManager();
				flushMemtableToDisk(memtable, sstableManager);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}		
	}
	
	/**
	 * Flush a memtable to disk
	 * @param memtable
	 * @param sstableManager 
	 * 
	 */
	protected void flushMemtableToDisk(final Memtable memtable, final TupleStoreManager sstableManager) {
		
		if(memtable == null) {
			logger.warn("Got null memtable, not flushing");
			return;
		}

		SSTableFacade facade = null;

		try {			
			// Don't write empty memtables to disk
			if (! memtable.isEmpty()) {
				final TupleStoreName sstableName = sstableManager.getTupleStoreName();
				final String dataDirectory = basedir.getAbsolutePath();
				final int tableNumber = writeMemtable(dataDirectory, memtable, sstableManager);
				
				final int sstableKeyCacheEntries = storage.getTupleStoreManagerRegistry().getConfiguration()
						.getSstableKeyCacheEntries();
				
				facade = new SSTableFacade(dataDirectory, sstableName, tableNumber, sstableKeyCacheEntries);
				facade.init();
			}
			
			sstableManager.replaceMemtableWithSSTable(memtable, facade);						
			sendCallbacks(memtable, sstableManager);	

			memtable.deleteOnClose();
			memtable.release();
		}  catch (Throwable e) {
			deleteWrittenFacade(facade);

			if(sstableManager.getSstableManagerState() == TupleStoreManagerState.READ_ONLY) {
				logger.debug("Rejected memtable write:", e);
				return;
			}
			
			logger.error("Exception while flushing memtable", e);
			
			if (Thread.currentThread().isInterrupted()) {
				logger.debug("Got Exception while flushing memtable, but thread was interrupted. "
						+ "Ignoring exception.");
				Thread.currentThread().interrupt();
				return;
			} 
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
	protected void sendCallbacks(final Memtable memtable, TupleStoreManager sstableManager) {
		final long timestamp = memtable.getCreatedTimestamp();
		final List<BiConsumer<TupleStoreName, Long>> callbacks 
			= storage.getTupleStoreManagerRegistry().getSSTableFlushCallbacks();
		
		for(final BiConsumer<TupleStoreName, Long> callback : callbacks) {
			try {
				callback.accept(sstableManager.getTupleStoreName(), timestamp);
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
			final TupleStoreManager sstableManager) throws Exception {
		
		final int tableNumber = sstableManager.increaseTableNumber();
		
		logger.info("Writing memtable number {} with {} entries and a size of {}", 
				tableNumber, 
				memtable.getNumberOfTuples(), 
				FileSizeHelper.readableFileSize(memtable.getSize()));

		try (final SSTableWriter ssTableWriter = new SSTableWriter(
				dataDirectory, sstableManager.getTupleStoreName(), tableNumber,
				memtable.getMaxEntries())) {

			ssTableWriter.open();
			ssTableWriter.addData(memtable.getSortedTupleList());
			return tableNumber;
		} catch (Exception e) {
			throw e;
		}
	}
}
/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
import org.bboxdb.storage.sstable.SSTableCreator;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemtableWriterRunnable extends ExceptionSafeRunnable {

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
	private final static Logger logger = LoggerFactory.getLogger(MemtableWriterRunnable.class);

	/**
	 * @param ssTableManager
	 */
	public MemtableWriterRunnable(final DiskStorage storage, final File basedir) {
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
		
		final boolean aquired = memtable.acquire();
		
		if(! aquired) {
			logger.error("Memtable should be flushed but can't aqired");
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

			// Schedule for deletion and release the global TupleStoreManager aquire
			memtable.deleteOnClose();
			memtable.release();
		}  catch (Throwable e) {
			handleErrorDuringMemtableWrite(sstableManager, facade, e); 
		} finally {
			// Release our aquire
			memtable.release();
		}
	}

	/**
	 * Handle and error during the memtable write
	 * @param sstableManager
	 * @param facade
	 * @param e
	 */
	private void handleErrorDuringMemtableWrite(final TupleStoreManager sstableManager, 
			final SSTableFacade facade, final Throwable e) {
		
		// Delete thr written facade
		if(facade != null) {
			facade.deleteOnClose();
		}
		
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
		final TupleStoreName tupleStoreName = sstableManager.getTupleStoreName();
		final long numberOfEntries = memtable.getNumberOfTuples();
		final String sizeString = FileSizeHelper.readableFileSize(memtable.getSize());
		
		logger.info("Writing memtable number {} of {} with {} entries and a size of {}", 
				tableNumber, tupleStoreName, numberOfEntries, sizeString);

		try (final SSTableWriter ssTableWriter = new SSTableWriter(
				dataDirectory, tupleStoreName, tableNumber, numberOfEntries, SSTableCreator.MEMTABLE)) {

			ssTableWriter.open();
			ssTableWriter.addTuples(memtable.getSortedTupleList());
			return tableNumber;
		} catch (Exception e) {
			throw e;
		}
	}
}
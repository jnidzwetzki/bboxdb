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
package org.bboxdb.storage.sstable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;

import org.bboxdb.commons.FileSizeHelper;
import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.commons.io.UnsafeMemoryHelper;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableCheckpointThread extends ExceptionSafeThread {

	/**
	 * The storage
	 */
	protected DiskStorage storage;
	
	/**
	 * The maximal number of seconds for data to stay in memory
	 */
	protected final long maxUncheckpointedMiliseconds;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCheckpointThread.class);

	public SSTableCheckpointThread(final DiskStorage storage, final int maxUncheckpointedSeconds) {
		this.storage = storage;
		this.maxUncheckpointedMiliseconds = TimeUnit.SECONDS.toMillis(maxUncheckpointedSeconds);
	}

	/**
	 * Execute the checkpoint thread
	 */
	protected void runThread() {

		final TupleStoreManagerRegistry storageRegistry = storage.getTupleStoreManagerRegistry();

		while(! Thread.currentThread().isInterrupted()) {
			
			if(Const.LOG_MEMORY_STATISTICS) {
				logMemoryStatistics();
			}
			
			final List<TupleStoreName> allTables = storageRegistry.getTupleStoresForLocation(
					storage.getBasedir().getAbsolutePath());
	
			for(final TupleStoreName ssTableName : allTables) {
				logger.debug("Executing checkpoint check for: {}", ssTableName);
				
				if(Thread.currentThread().isInterrupted()) {
					return;
				}
				
				createCheckpointIfNedded(storageRegistry, ssTableName);
			}
			
			waitForNextRun();
		}
	}

	/**
	 * Wait for the next thread run
	 */
	protected void waitForNextRun() {
		try {
			Thread.sleep(SSTableConst.CHECKPOINT_THREAD_DELAY);
		} catch (InterruptedException e) {
			logger.info("Chekpoint thread was interrupted");
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Create a checkpoint if needed
	 * @param storageRegistry
	 * @param ssTableName
	 */
	protected void createCheckpointIfNedded(final TupleStoreManagerRegistry storageRegistry, 
			final TupleStoreName ssTableName) {
		try {
			final TupleStoreManager ssTableManager = storageRegistry.getTupleStoreManager(ssTableName);
			createCheckpoint(ssTableManager);
		} catch (InterruptedException e) {
			logger.debug("Got interrupted exception, stopping checkpoint thread");
			Thread.currentThread().interrupt();
		} catch (StorageManagerException e) {
			logger.error("Got exception while creating checkpoint");
		}
	}
	
	@Override
	protected void beginHook() {
		logger.info("Checkpoint thread has started");
	}
	
	@Override
	protected void endHook() {
		logger.info("Checkpoint thread has stopped");
	}
	
	/**
	 * Decide if a new checkpoint is needed
	 * @return
	 */
	protected boolean isCheckpointNeeded(final TupleStoreManager ssTableManager) {
		
		final List<ReadOnlyTupleStore> inMemoryStores 
			= ssTableManager.getAllInMemoryStorages();
	
		if(inMemoryStores.isEmpty()) {
			return false;
		}
		
		final long currentTime = System.currentTimeMillis();
	
		// The checkpoint predicate
		final LongPredicate checkpointPredicate = m -> {
			final long checkpointThreshold = TimeUnit.MICROSECONDS.toMillis(m) + maxUncheckpointedMiliseconds;
			return checkpointThreshold < currentTime;
		};
		
		final boolean checkpointNeeded = inMemoryStores
				.stream()
				.filter(Objects::nonNull)
				.mapToLong(m -> m.getOldestTupleVersionTimestamp())
				.anyMatch(checkpointPredicate);
		
		return checkpointNeeded;
	}

	/**
	 * Create a new checkpoint, this means flush all old memtables to disk
	 * @throws InterruptedException 
	 */
	protected void createCheckpoint(final TupleStoreManager ssTableManager) throws InterruptedException {
		if(isCheckpointNeeded(ssTableManager)) {
			final String fullname = ssTableManager.getTupleStoreName().getFullname();
			logger.debug("Create a checkpoint for: {}", fullname);
			ssTableManager.flush();
			logger.debug("Create checkpoint DONE for: {}", fullname);
		}
	}
	
	
	/**
	 * Log statistics about memory consumption
	 */
	protected void logMemoryStatistics() {
		final long totalMemory = Runtime.getRuntime().totalMemory();
		final long freeMemory = Runtime.getRuntime().freeMemory();
		final long maxMemory = Runtime.getRuntime().maxMemory();
		final long usedMemory = totalMemory - freeMemory;
		
		logger.info("Maximum memory: {}, Total memory: {}, "
				+ "Free memory within total: {}, Used memory {}", 
				FileSizeHelper.readableFileSize(maxMemory),
				FileSizeHelper.readableFileSize(totalMemory), 
				FileSizeHelper.readableFileSize(freeMemory), 
				FileSizeHelper.readableFileSize(usedMemory));
		
		try {
			final long mappedBytes = UnsafeMemoryHelper.getMappedBytes();
			logger.info("Memory mapped segments: {}, memory mapped data: {}",
					UnsafeMemoryHelper.getMappedSegments(),
					FileSizeHelper.readableFileSize(mappedBytes));
		} catch (Exception e) {
			logger.debug("Unable to get memory statistics", e);
		}
	}
}

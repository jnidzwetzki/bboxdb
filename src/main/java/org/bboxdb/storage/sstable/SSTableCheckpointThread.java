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
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.bboxdb.storage.Memtable;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.util.ExceptionSafeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableCheckpointThread extends ExceptionSafeThread {

	/**
	 * The sstable manager
	 */
	protected final SSTableManager ssTableManager;
	
	/**
	 * The maximal number of seconds for data to stay in memory
	 */
	protected final long maxUncheckpointedMiliseconds;

	/**
	 * The name of the thread
	 */
	protected final String threadname;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCheckpointThread.class);

	public SSTableCheckpointThread(final int maxUncheckpointedSeconds, final SSTableManager ssTableManager) {
		this.maxUncheckpointedMiliseconds = TimeUnit.SECONDS.toMillis(maxUncheckpointedSeconds);
		this.ssTableManager = ssTableManager;
		this.threadname = ssTableManager.getSSTableName().getFullname();
	}

	/**
	 * Execute the ceckpoint thread
	 */
	protected void runThread() {
		logger.info("Checkpoint thread has started: {} ", threadname);

		while(! Thread.currentThread().isInterrupted()) {
			logger.debug("Executing checkpoint thread for: {}", threadname);
		
			try {
				createCheckpoint();
				Thread.sleep(SSTableConst.CHECKPOINT_THREAD_DELAY);
			} catch (InterruptedException e) {
				logger.debug("Got interrupted exception, stopping checkpoint thread for: {}", threadname);
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
	
	@Override
	protected void endHook() {
		logger.info("Checkpoint thread has stopped: {} ", threadname);
	}
	
	/**
	 * Decide if a new checkpoint is needed
	 * @return
	 */
	protected boolean isCheckpointNeeded() {
		
		final List<ReadOnlyTupleStorage> inMemoryStores 
			= ssTableManager.getTupleStoreInstances().getAllInMemoryStorages();
		
		final long currentTime = System.currentTimeMillis();
	
		final boolean checkpointNeeded = inMemoryStores
				.stream()
				.mapToLong(m -> m.getOldestTupleVersionTimestamp())
				.anyMatch(m -> 
					(TimeUnit.MICROSECONDS.toMillis(m) + maxUncheckpointedMiliseconds)
				    < currentTime);
		
		return checkpointNeeded;
	}

	/**
	 * Create a new checkpoint, this means flush all old memtables to disk
	 * @throws InterruptedException 
	 */
	protected void createCheckpoint() throws InterruptedException {
	
		if(isCheckpointNeeded()) {
			final Memtable activeMemtable = ssTableManager.getMemtable();
			logger.debug("Create a checkpoint for: {}", threadname);
			ssTableManager.flushAndInitMemtable();
			
			final Queue<Memtable> unflushedMemtables 
				= ssTableManager.getTupleStoreInstances().getMemtablesToFlush();
			
			// Wait until the active memtable is flushed to disk
			synchronized (unflushedMemtables) {
				while(unflushedMemtables.contains(activeMemtable)) {
					unflushedMemtables.wait();
				}
			}
			
			logger.info("Create checkpoint DONE for: {}", threadname);
		}		
	}
}

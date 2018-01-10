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
package org.bboxdb.storage.tuplestore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.concurrent.ThreadHelper;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.storage.entity.MemtableAndTupleStoreManagerPair;
import org.bboxdb.storage.memtable.MemtableWriterThread;
import org.bboxdb.storage.sstable.SSTableCheckpointThread;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.compact.SSTableCompactorThread;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Gauge;

public class DiskStorage implements BBoxDBService {

	/**
	 * The running flush threads
	 */
	public final List<Thread> runningThreads = new ArrayList<>();
	
	/**
	 * The state of the service
	 */
	protected final ServiceState serviceState = new ServiceState();
	
	/**
	 * The queue for the memtable flush thread
	 */
	protected final BlockingQueue<MemtableAndTupleStoreManagerPair> memtablesToFlush;
	
	/**
	 * The storage base dir
	 */
	protected final File basedir;
	
	/**
	 * Number of flush threads per storage
	 */
	protected int flushThreadsPerStorage;
	
	/**
	 * The tuple store manager registry
	 */
	protected final TupleStoreManagerRegistry tupleStoreManagerRegistry;
	
	/**
	 * The storage label
	 */
	protected final String performanceCounterLabel;
	
	/**
	 * The unflushed memtable total counter
	 */
	protected final static Gauge unflushedMemtablesTotal = Gauge.build()
			.name("bboxdb_unflushed_memtables_total")
			.help("Total unflushed memtables")
			.labelNames("storage").register();
	
	/**
	 * The unflushed memtable bytes counter
	 */
	protected final static Gauge unflushedMemtablesBytes = Gauge.build()
			.name("bboxdb_unflushed_memtables_bytes")
			.help("Unflushed memtable bytes")
			.labelNames("storage").register();
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DiskStorage.class);
	
	public DiskStorage(final TupleStoreManagerRegistry storageRegistry, 
			final File basedir, 
			final int flushThreadsPerStorage) {
		
		this.tupleStoreManagerRegistry = storageRegistry;
		this.basedir = basedir;
		this.flushThreadsPerStorage = flushThreadsPerStorage;
		this.memtablesToFlush = new ArrayBlockingQueue<>(SSTableConst.MAX_UNFLUSHED_MEMTABLES_PER_TABLE);
		
		this.performanceCounterLabel = basedir.toString();
	}

	@Override
	public void init() {
		if(serviceState.isInRunningState()) {
			logger.warn("Unable to init service, is already in {} state", serviceState);
			return;
		}
		
		serviceState.dipatchToStarting();
		memtablesToFlush.clear();
	
		startFlushThreads();
		startCompactThread();
		startCheckpointThread();
		
		serviceState.dispatchToRunning();
	}

	/**
	 * Start the flush threads
	 */
	protected void startFlushThreads() {
		for(int i = 0; i < flushThreadsPerStorage; i++) {
			final String threadname = i + ". Memtable write thread for storage: " + basedir;
			
			final MemtableWriterThread memtableWriterThread = new MemtableWriterThread(
					this, basedir);
			
			final Thread thread = new Thread(memtableWriterThread);
			thread.setName(threadname);
			thread.start();
			runningThreads.add(thread);
		}
	}
	
	/**
	 * Start the compact thread if needed
	 */
	protected void startCompactThread() {
		final SSTableCompactorThread sstableCompactor = new SSTableCompactorThread(this);
		final Thread compactThread = new Thread(sstableCompactor);
		compactThread.setName("Compact thread for: " + basedir);
		compactThread.start();
		runningThreads.add(compactThread);
	}
	
	/**
	 * Start the checkpoint thread for the storage
	 */
	protected void startCheckpointThread() {
		final BBoxDBConfiguration configuration = tupleStoreManagerRegistry.getConfiguration();
		if(configuration.getStorageCheckpointInterval() > 0) {
			final int maxUncheckpointedSeconds = configuration.getStorageCheckpointInterval();
			final SSTableCheckpointThread ssTableCheckpointThread = new SSTableCheckpointThread(this, maxUncheckpointedSeconds);
			final Thread checkpointThread = new Thread(ssTableCheckpointThread);
			checkpointThread.setName("Checkpoint thread for: " + basedir);
			checkpointThread.start();
			runningThreads.add(checkpointThread);
		}
	}

	@Override
	public void shutdown() {
		
		if(! serviceState.isInRunningState()) {
			logger.warn("Unable to stop service, is already in {} state", serviceState);
			return;
		}
		
		serviceState.dispatchToStopping();
		
		logger.info("Stop running threads");
		ThreadHelper.stopThreads(runningThreads);
		
		runningThreads.clear();
		serviceState.dispatchToTerminated();
	}

	@Override
	public String getServicename() {
		return "Storage instance for: " + basedir.getAbsolutePath();
	}
	
	/**
	 * Schedule a memtable flush
	 * @param memtable
	 */
	public void scheduleMemtableFlush(final MemtableAndTupleStoreManagerPair memtable) {		
		// The put call can block when more than
		// MAX_UNFLUSHED_MEMTABLES_PER_TABLE are unflushed.
		try {
			memtablesToFlush.put(memtable);
			
			// Update performance counter
			unflushedMemtablesTotal.labels(performanceCounterLabel).inc();
			unflushedMemtablesBytes.labels(performanceCounterLabel).inc(memtable.getMemtable().getSize());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Get the next memtable to flush
	 * @return
	 * @throws InterruptedException 
	 */
	public MemtableAndTupleStoreManagerPair takeNextUnflushedMemtable() throws InterruptedException {
		final MemtableAndTupleStoreManagerPair memtable = memtablesToFlush.take();
		
		// Update performance counter
		unflushedMemtablesTotal.labels(performanceCounterLabel).dec();
		unflushedMemtablesBytes.labels(performanceCounterLabel).dec(memtable.getMemtable().getSize());
		
		return memtable;
	}
	
	/**
	 * Get the basedir of this storage
	 * @return
	 */
	public File getBasedir() {
		return basedir;
	}

	/**
	 * Get the storage registry
	 * @return
	 */
	public TupleStoreManagerRegistry getTupleStoreManagerRegistry() {
		return tupleStoreManagerRegistry;
	}
}

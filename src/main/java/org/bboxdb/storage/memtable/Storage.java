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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.storage.registry.MemtableAndSSTableManager;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.util.ServiceState;
import org.bboxdb.util.ThreadHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Storage implements BBoxDBService {

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
	protected final BlockingQueue<MemtableAndSSTableManager> memtablesToFlush;
	
	/**
	 * The storage base dir
	 */
	protected final File basedir;
	
	/**
	 * Number of flush threads per storage
	 */
	protected int flushThreadsPerStorage;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Storage.class);
	
	public Storage(final File basedir, final int flushThreadsPerStorage) {
		this.basedir = basedir;
		this.flushThreadsPerStorage = flushThreadsPerStorage;
		this.memtablesToFlush = new ArrayBlockingQueue<>(SSTableConst.MAX_UNFLUSHED_MEMTABLES_PER_TABLE);
	}

	@Override
	public void init() {

		if(serviceState.isInRunningState()) {
			logger.warn("Unable to init service, is already in {} state", serviceState);
			return;
		}
		
		serviceState.dipatchToStarting();
		memtablesToFlush.clear();
		
		for(int i = 0; i < flushThreadsPerStorage; i++) {
			final String threadname = i + ". Memtable write thread for storage: " + basedir;
			
			final MemtableWriterThread memtableWriterThread = new MemtableWriterThread(
					memtablesToFlush, basedir);
			
			final Thread thread = new Thread(memtableWriterThread);
			thread.setName(threadname);
			thread.start();
			runningThreads.add(thread);
		}
		
		serviceState.dispatchToRunning();
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
	
	public void scheduleMemtableFlush(final MemtableAndSSTableManager memtable) {
		// The put call can block when more than
		// MAX_UNFLUSHED_MEMTABLES_PER_TABLE are unflushed.
		//
		// So we wait otside of the synchonized area.
		// Because, otherwise no other threads could call
		// replaceMemtableWithSSTable() and reduce
		// the queue size
		if(memtable != null) {
			try {
				memtablesToFlush.put(memtable);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
	
	/**
	 * Get the memtable flush queue
	 * @return
	 */
	public BlockingQueue<MemtableAndSSTableManager> getMemtablesToFlush() {
		return memtablesToFlush;
	}
	
	/**
	 * Get the basedir of this storage
	 * @return
	 */
	public File getBasedir() {
		return basedir;
	}

}

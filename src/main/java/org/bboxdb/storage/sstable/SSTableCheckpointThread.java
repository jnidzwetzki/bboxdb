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
package org.bboxdb.storage.sstable;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.ScalephantException;
import org.bboxdb.storage.Memtable;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.util.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableCheckpointThread implements Runnable, Stoppable {

	/**
	 * The sstable manager
	 */
	protected final SSTableManager ssTableManager;
	
	/**
	 * The maximal number of seconds for data to stay in memory
	 */
	protected final long maxUncheckpointedMiliseconds;
	
	/**
	 * The run variable
	 */
	protected volatile boolean run;

	/**
	 * The name of the local instance
	 */
	protected DistributedInstance localInstance;
	
	/**
	 * The distribution region of the sstable
	 */
	protected DistributionRegion distributionRegion = null;
	
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
		this.run = true;
		
		this.threadname = ssTableManager.getSSTableName().getFullname();
		
		// Local instance
		final BBoxDBConfiguration scalephantConfiguration = BBoxDBConfigurationManager.getConfiguration();
		this.localInstance = ZookeeperClientFactory.getLocalInstanceName(scalephantConfiguration);
	
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
			
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForTableName(
					ssTableManager.getSSTableName().getFullname(), zookeeperClient);

			final DistributionRegion distributionGroupRoot = distributionAdapter.getRootNode();
			
			distributionRegion = DistributionRegionHelper.getDistributionRegionForNamePrefix(distributionGroupRoot, ssTableManager.getSSTableName().getNameprefix());
		} catch (ZookeeperException | ScalephantException e) {
			logger.warn("Unable to find distribution region: " , e);
		}
	}

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

	protected void executeThread() {
		while(run) {
			logger.debug("Executing checkpoint thread for: {}", threadname);
		
			try {
				createCheckpoint();
				Thread.sleep(SSTableConst.CHECKPOINT_THREAD_DELAY);
			} catch (InterruptedException e) {
				logger.info("Got interrupted exception, stopping checkpoint thread for: {}", threadname);
				Thread.currentThread().interrupt();
				return;
			}
		}
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
				.anyMatch(m -> (m.getOldestTupleTimestamp() + maxUncheckpointedMiliseconds) < currentTime);
		
		return checkpointNeeded;
	}

	/**
	 * Create a new checkpoint, this means flush all old memtables to disk
	 * @throws InterruptedException 
	 */
	protected void createCheckpoint() throws InterruptedException {
		try {

			if(isCheckpointNeeded()) {
				final Memtable activeMemtable = ssTableManager.getMemtable();
				logger.info("Creating a checkpoint for: {}", threadname);
				ssTableManager.flushMemtable();
				
				final Queue<Memtable> unflushedMemtables = ssTableManager.getTupleStoreInstances().getMemtablesToFlush();
				
				// Wait until the active memtable is flushed to disk
				synchronized (unflushedMemtables) {
					while(unflushedMemtables.contains(activeMemtable)) {
						unflushedMemtables.wait();
					}
				}
				
				logger.info("Create checkpoint DONE for: {}", threadname);
			}
			
			// Update checkpoint date in zookeeper
			final Memtable activeMemtable = ssTableManager.getMemtable();
			final long createdTimestamp = activeMemtable.getCreatedTimestamp();
			updateCheckpointDate(createdTimestamp);
			
		} catch (ZookeeperException | StorageManagerException e) {
			logger.warn("Got an exception while creating checkpoint", e);
		}
	}

	/**
	 * Update the checkpoint date (e.g. propagate checkpoint to zookeeper)
	 * @param createdTimestamp
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	protected void updateCheckpointDate(final long checkpointTimestamp) throws ZookeeperException, InterruptedException {
		
		logger.debug("Updating checkpoint for: {} to {}", threadname, checkpointTimestamp);
		
		if(distributionRegion != null) {
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(distributionRegion, localInstance, checkpointTimestamp);
		}
	}

	@Override
	public void stop() {
		run = false;
	}

}

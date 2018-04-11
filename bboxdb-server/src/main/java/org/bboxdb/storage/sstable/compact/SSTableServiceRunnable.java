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
package org.bboxdb.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.RegionMergeHelper;
import org.bboxdb.distribution.partitioner.regionsplit.RegionMerger;
import org.bboxdb.distribution.partitioner.regionsplit.RegionSplitHelper;
import org.bboxdb.distribution.partitioner.regionsplit.RegionSplitter;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.sstable.reader.SSTableKeyIndexReader;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class SSTableServiceRunnable extends ExceptionSafeRunnable {
	
	/**
	 * The merge strategy
	 */
	protected final MergeStrategy mergeStrategy;

	/**
	 * The storage
	 */
	protected final DiskStorage storage;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableServiceRunnable.class);

	public SSTableServiceRunnable(final DiskStorage storage) {
		this.storage = storage;
		this.mergeStrategy = new SimpleMergeStrategy();
	}

	@Override
	protected void beginHook() {
		logger.info("SSTable service thread for {} has started", storage.getBasedir());
	}
	
	@Override
	protected void endHook() {
		logger.info("SSTable service thread for {} is DONE", storage.getBasedir());
	}
	
	/**
	 * Execute the compactor thread
	 */
	protected void runThread() {			
		while(! Thread.currentThread().isInterrupted()) {
			try {	
				Thread.sleep(mergeStrategy.getCompactorDelay());
				logger.debug("Executing compact thread");
				execute(); 
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			} 
		}		
	}

	/**
	 * Execute a new compaction
	 * @throws InterruptedException 
	 */
	public synchronized void execute() throws InterruptedException {
		
		final TupleStoreManagerRegistry storageRegistry = storage.getTupleStoreManagerRegistry();
		final String location = storage.getBasedir().getAbsolutePath();
		final List<TupleStoreName> tupleStores = storageRegistry.getTupleStoresForLocation(location);
				
		processTupleStores(storageRegistry, tupleStores);
		processRegionMerges();
		processScheduldedDeletions();
	}

	/**
	 * Process the schedules table deletions
	 */
	private void processScheduldedDeletions() {
		final LinkedTransferQueue<TupleStoreName> pendingTableDeletions 
			= storage.getPendingTableDeletions();
		
		while(! pendingTableDeletions.isEmpty()) {
			final TupleStoreName tableName = pendingTableDeletions.poll();
			if(tableName != null) {
				logger.info("Deleting table {}", tableName.getFullname());
				TupleStoreManager.deletePersistentTableData(storage.getBasedir().getAbsolutePath(), tableName);
			}
		}
	}

	/**
	 * Process the tuple stores
	 * 
	 * @param storageRegistry
	 * @param tupleStores
	 * @throws InterruptedException
	 */
	private void processTupleStores(final TupleStoreManagerRegistry storageRegistry,
			final List<TupleStoreName> tupleStores) throws InterruptedException {
		
		for(final TupleStoreName tupleStoreName: tupleStores) {
			try {
				logger.debug("Running compact for: {}", tupleStoreName);
				final TupleStoreManager tupleStoreManager = storageRegistry.getTupleStoreManager(tupleStoreName);
				
				if(tupleStoreManager.getSstableManagerState() == TupleStoreManagerState.READ_ONLY) {
					logger.debug("Skipping compact for read only sstable manager: {}" , tupleStoreName);
					continue;
				}
			
				if(! CompactorHelper.isRegionActive(tupleStoreName)) {
					logger.info("Skipping compact run, because region is not active {}", tupleStoreName);
					continue;
				}
			
				final List<SSTableFacade> facades = getAllTupleStores(tupleStoreManager);
				final MergeTask mergeTask = mergeStrategy.getMergeTask(facades);
				executeCompactTask(mergeTask, tupleStoreManager);
				testForRegionOverflow(tupleStoreManager);
				
			} catch (StorageManagerException | BBoxDBException e) {
				logger.error("Error while merging tables", e);	
			} 
		}		
	}

	/**
	 * Test for region merge
	 * 
	 * @throws BBoxDBException
	 */
	private void processRegionMerges() {
		final Set<String> groups = SpacePartitionerCache.getInstance().getAllKnownDistributionGroups();
		
		for(final String groupName : groups) {
			try {
				testForMergeInGroup(groupName);
			} catch (BBoxDBException e) {
				logger.error("Got an exception while testing for region merge {}", groupName, e);
			}
		}
	}

	/**
	 * Test for merge in distribution region
	 * 
	 * @param localinstance
	 * @param groupName
	 * @throws BBoxDBException
	 */
	private void testForMergeInGroup(final String groupName) throws BBoxDBException {
		
		final BBoxDBInstance localinstance = ZookeeperClientFactory.getLocalInstanceName();
		final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(groupName);
		
		final DistributionRegion rootNode = spacePartitioner.getRootNode();
		
		if(rootNode == null) {
			return;
		}
		
		final List<DistributionRegion> mergeCandidates = rootNode
				.getThisAndChildRegions()
				.stream()
				.filter(r -> r.getSystems().contains(localinstance))
				.collect(Collectors.toList());
		
		for(final DistributionRegion region : mergeCandidates) {
			testForUnderflow(spacePartitioner, region);
		}
	}
	


	/**
	 * Create a copy to ensure, that the list of facades don't change
	 * during the compact run.
	 * 
	 * @param tupleStoreManager
	 * @return
	 */
	private List<SSTableFacade> getAllTupleStores(final TupleStoreManager tupleStoreManager) {
		final List<SSTableFacade> facades = new ArrayList<>();
		facades.addAll(tupleStoreManager.getSstableFacades());
		return facades;
	}

	/**
	 * Merge multiple facades into a new one
	 * @param sstableManager 
	 *
	 * @throws StorageManagerException
	 * @throws InterruptedException 
	 * @throws ZookeeperException 
	 * @throws BBoxDBException 
	 */
	private void executeCompactTask(final MergeTask mergeTask, final TupleStoreManager sstableManager) 
			throws StorageManagerException, BBoxDBException, InterruptedException {
		
		if(mergeTask.getTaskType() == MergeTaskType.UNKNOWN) {
			return;
		}
		
		final List<SSTableFacade> facades = mergeTask.getCompactTables();
	
		if(facades == null || facades.isEmpty()) {
			return;
		}
		
		final List<SSTableKeyIndexReader> reader = mergeTask.getCompactTables()
				.stream()
				.map(f -> f.getSsTableKeyIndexReader())
				.collect(Collectors.toList());
		
		// Log the compact call
		final boolean majorCompaction = mergeTask.getTaskType() == MergeTaskType.MAJOR;
		if(logger.isInfoEnabled()) {
			writeMergeLog(facades, majorCompaction);
		}
		
		// Run the compact process
		final SSTableCompactor ssTableCompactor = new SSTableCompactor(sstableManager, reader);
		ssTableCompactor.setMajorCompaction(majorCompaction);
		ssTableCompactor.executeCompactation();
		final List<SSTableWriter> newTables = ssTableCompactor.getResultList();

		final float mergeFactor = (float) ssTableCompactor.getWrittenTuples() / (float) ssTableCompactor.getReadTuples();
		
		logger.info("Compactation done. Read {} tuples, wrote {} tuples. Factor {}", 
				ssTableCompactor.getReadTuples(), ssTableCompactor.getWrittenTuples(), 
				mergeFactor);
		
		registerNewFacadeAndDeleteOldInstances(sstableManager, facades, newTables);		
	}

	/**
	 * Does the region needs to be split?
	 * @param sstableManager
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private void testForRegionOverflow(final TupleStoreManager sstableManager)
			throws BBoxDBException, InterruptedException {
		
		// Don't try to split non-distributed tables
		if(! sstableManager.getTupleStoreName().isDistributedTable()) {
			return;
		}

		try {
			final TupleStoreName ssTableName = sstableManager.getTupleStoreName();
			final long regionId = ssTableName.getRegionId().getAsLong();
			final SpacePartitioner spacePartitioner = getSpacePartitioner(ssTableName);
			final DistributionRegion distributionRegion = spacePartitioner.getRootNode();

			final DistributionRegion regionToSplit = DistributionRegionHelper
					.getDistributionRegionForNamePrefix(distributionRegion, regionId);
			
			// Region does not exist
			if(regionToSplit == null) {
				return;
			}
			
			if(! RegionSplitHelper.isSplittingSupported(regionToSplit)) {
				return;
			}
					
			if(! RegionSplitHelper.isRegionOverflow(regionToSplit)) {
				return;
			}
			
			executeSplit(sstableManager, spacePartitioner, regionToSplit);			
		} catch (Exception e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Execute a region split
	 * 
	 * @param sstableManager
	 * @param spacePartitioner
	 * @param regionToSplit
	 * @throws StorageManagerException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private void executeSplit(final TupleStoreManager sstableManager, final SpacePartitioner spacePartitioner,
			final DistributionRegion regionToSplit) throws Exception {
		
		final TupleStoreManagerRegistry tupleStoreManagerRegistry = storage.getTupleStoreManagerRegistry();
		final RegionSplitter regionSplitter = new RegionSplitter(tupleStoreManagerRegistry);

		forceMajorCompact(sstableManager);
		
		regionSplitter.splitRegion(regionToSplit, spacePartitioner, tupleStoreManagerRegistry);
	}

	/**
	 * Get the space partitioner
	 * 
	 * @param ssTableName
	 * @return
	 * @throws BBoxDBException
	 */
	private SpacePartitioner getSpacePartitioner(final TupleStoreName ssTableName) throws BBoxDBException {
		
		final String distributionGroup = ssTableName.getDistributionGroup();
		return SpacePartitionerCache.getInstance()
				.getSpacePartitionerForGroupName(distributionGroup);
	}

	/**
	 * Test for region underflow
	 * 
	 * @param spacePartitioner
	 * @param regionToMerge
	 * @throws BBoxDBException
	 */
	private void testForUnderflow(final SpacePartitioner spacePartitioner, 
			final DistributionRegion oneSourceNode) throws BBoxDBException {
				
		final List<List<DistributionRegion>> candidates = spacePartitioner.getMergeCandidates(oneSourceNode);
		final BBoxDBInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName();
		
		for(final List<DistributionRegion> sources : candidates) {
		
			if(RegionMergeHelper.isRegionUnderflow(sources, localInstanceName)) {
				final TupleStoreManagerRegistry tupleStoreManagerRegistry = storage.getTupleStoreManagerRegistry();
				final RegionMerger regionMerger = new RegionMerger(tupleStoreManagerRegistry);
				regionMerger.mergeRegion(sources, spacePartitioner, tupleStoreManagerRegistry);	
				return;
			}	
		}
	}

	/**
	 * @param sstableManager
	 * @throws StorageManagerException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	@VisibleForTesting
	public void forceMajorCompact(final TupleStoreManager sstableManager)
			throws StorageManagerException, BBoxDBException, InterruptedException {
		
		logger.info("Force major compact for {}", sstableManager.getTupleStoreName().getFullname());
		
		final MergeTask mergeTask = new MergeTask();
		mergeTask.setTaskType(MergeTaskType.MAJOR);
		mergeTask.setCompactTables(getAllTupleStores(sstableManager));
		executeCompactTask(mergeTask, sstableManager);
	}

	/**
	 * Register a new sstable facade and delete the old ones
	 * @param oldFacades
	 * @param directory
	 * @param name
	 * @param tablenumber
	 * @throws StorageManagerException
	 */
	private void registerNewFacadeAndDeleteOldInstances(final TupleStoreManager sstableManager, 
			final List<SSTableFacade> oldFacades, 
			final List<SSTableWriter> newTableWriter) throws StorageManagerException {
		
		final List<SSTableFacade> newFacades = new ArrayList<>();
		
		// Open new facades
		openFacades(newTableWriter, newFacades);
		
		// Manager has switched to read only
		if(sstableManager.getSstableManagerState() == TupleStoreManagerState.READ_ONLY) {
			logger.info("Manager is in read only mode, cancel compact run");
			handleCompactException(newFacades);
			return;
		}
		
		try {
			for(final SSTableFacade facade : newFacades) {
				facade.init();
			}
			
			// Switch facades in registry
			sstableManager.replaceCompactedSStables(newFacades, oldFacades);

			// Schedule facades for deletion
			oldFacades.forEach(f -> f.deleteOnClose());
		} catch (BBoxDBException | RejectedException e) {
			handleCompactException(newFacades);
			throw new StorageManagerException(e);
		} catch (InterruptedException e) {
			handleCompactException(newFacades);
			Thread.currentThread().interrupt();
			throw new StorageManagerException(e);
		} 
	}

	/**
	 * Open the facades
	 * 
	 * @param newTableWriter
	 * @param newFacades
	 * @throws StorageManagerException
	 */
	private void openFacades(final List<SSTableWriter> newTableWriter, final List<SSTableFacade> newFacades)
			throws StorageManagerException {
		
		for(final SSTableWriter writer : newTableWriter) {
			
			final TupleStoreManagerRegistry tupleStoreManagerRegistry = storage.getTupleStoreManagerRegistry();
			final BBoxDBConfiguration configuration = tupleStoreManagerRegistry.getConfiguration();
			final int sstableKeyCacheEntries = configuration.getSstableKeyCacheEntries();
			
			final SSTableFacade newFacade = new SSTableFacade(writer.getDirectory(), 
					writer.getName(), writer.getTablenumber(), sstableKeyCacheEntries);
			
			newFacades.add(newFacade);
		}
	}
	
	/** 
	 * Handle exception in compact thread
	 * @param newFacades
	 */
	@VisibleForTesting
	public void handleCompactException(final List<SSTableFacade> newFacades) {
		logger.error("Got an exception, schedule delete for {} partially written tables", 
				newFacades.size());
		
		for(final SSTableFacade facade : newFacades) {
			facade.deleteOnClose();
			assert (facade.getUsage().get() == 0) : "Usage counter is not 0 " + facade.getInternalName();
		}
	}

	/**
	 * Write info about the merge run into log
	 * @param facades
	 * @param tablenumber
	 */
	private void writeMergeLog(final List<SSTableFacade> facades, final boolean majorCompaction) {
		
		final String formatedFacades = facades
				.stream()
				.mapToInt(SSTableFacade::getTablebumber)
				.mapToObj(Integer::toString)
				.collect(Collectors.joining(",", "[", "]"));
		
		logger.info("Merging (major: {}) {}", majorCompaction, formatedFacades);
	}
}

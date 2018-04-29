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
package org.bboxdb.distribution.partitioner.regionsplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.tuplestore.manager.TupleStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitter {

	/**
	 * The storage reference
	 */
	private final TupleStoreManagerRegistry registry;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionSplitter.class);
	
	public RegionSplitter(final TupleStoreManagerRegistry registry) {
		assert (registry != null) : "Unable to init, registry is null";		
		this.registry = registry;
	}
	
	/**
	 * Perform a distribution region split
	 * 
	 * @param region
	 * @param distributionGroupZookeeperAdapter
	 * @param spacePartitioner
	 * @param diskStorage
	 */
	public void splitRegion(final DistributionRegion region, final SpacePartitioner spacePartitioner, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) {
		
		assert(region != null);
	
		final DistributionRegionAdapter distributionRegionZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();


		logger.info("Performing split for: {}", region.getIdentifier());
		
		final boolean setResult = tryToSetToFullSplitting(region, distributionRegionZookeeperAdapter);
		
		if(! setResult) {
			return;
		}
		
		boolean splitFailed = false;
		final List<DistributionRegion> destination = new ArrayList<>();
		
		try {
			final Collection<Hyperrectangle> samples 
				= SamplingHelper.getSamplesForRegion(region, tupleStoreManagerRegistry);
			
			final List<DistributionRegion> splitRegions = spacePartitioner.splitRegion(region, samples);
			destination.addAll(splitRegions);
	
			redistributeDataSplit(region, destination);
			distributionRegionZookeeperAdapter.deleteRegionStatistics(region);
			
			// Setting the region to split will cause a local data delete (see TupleStoreZookeeperObserver)
			spacePartitioner.splitComplete(region, destination);
		} catch (Throwable e) {
			logger.warn("Got exception during split, retry in a few minutes: " + region.getIdentifier(), e);
			splitFailed = true;
		}

		handleSplitFailed(region, spacePartitioner, splitFailed, destination);
		
		logger.info("Performing split for: {} is done", region.getIdentifier());
	}

	/**
	 * Handle a failed split 
	 * @param region
	 * @param spacePartitioner
	 * @param splitFailed
	 * @param destination 
	 */
	private void handleSplitFailed(final DistributionRegion region, final SpacePartitioner spacePartitioner,
			final boolean splitFailed, final List<DistributionRegion> destination) {
		
		if(! splitFailed) {
			return;
		}
		
		try {
			spacePartitioner.splitFailed(region, destination);
		} catch (BBoxDBException e) {
			logger.error("Got exception while resetting area state for to active, "
					+ "your global index might be inconsistent now "+ region.getIdentifier(), e);
		}
	}

	/**
	 * Try to set the region split state
	 * 
	 * @param region
	 * @param distributionRegionAdapter
	 * @return 
	 */
	private boolean tryToSetToFullSplitting(final DistributionRegion region,
			final DistributionRegionAdapter distributionRegionAdapter) {
		
		try {
			// Try to set region state to full. If this fails, another node is already 
			// splits the region
			final boolean setToFullResult = distributionRegionAdapter.setToFull(region);
			
			if(! setToFullResult) {
				final DistributionRegionState stateForDistributionRegion 
					= distributionRegionAdapter.getStateForDistributionRegion(region);
				
				logger.info("Unable to set state to full for region: {}, stopping split. Old state was {}", 
						region.getIdentifier(), stateForDistributionRegion);
				
				return false;
			}
		} catch (Throwable e) {
			logger.warn("Got uncought exception during split: " + region.getIdentifier(), e);
			return false;
		}
		
		return true;
	}
	

	/**
	 * Redistribute data after region split
	 * @param region
	 */
	private void redistributeDataSplit(final DistributionRegion source, 
			final List<DistributionRegion> destination) {
		
		final long regionId = source.getRegionId();
		
		try {
			logger.info("Redistributing all data for region: {}", regionId);
						
			final String distributionGroupName = source.getDistributionGroupName();
			
			final List<TupleStoreName> localTables = TupleStoreUtil
					.getAllTablesForDistributionGroupAndRegionId(registry, distributionGroupName, regionId);
	
			// Remove the local mapping, no new data is written to the region
			final SpacePartitioner spacePartitioner = SpacePartitionerCache
					.getInstance().getSpacePartitionerForGroupName(distributionGroupName);
			
			final DistributionRegionIdMapper mapper = spacePartitioner.getDistributionRegionIdMapper();
			
			// We have set the region to splitting, wait until we see this status change 
			// from Zookeeper and the space partitioner removed this region as active
			mapper.waitUntilMappingDisappears(regionId);
			
			// Redistribute data
			for(final TupleStoreName ssTableName : localTables) {
				// Reject new writes and flush to disk
				stopFlushToDisk(ssTableName);
				distributeData(ssTableName, source, destination);	
			}

		} catch (InterruptedException e) {
			logger.warn("Thread was interrupted");
			Thread.currentThread().interrupt();
			return;
		} catch (Exception e) {
			logger.error("Got exception when redistribute local data", e);
			return;
		}
		
		logger.info("Redistributing data for region: {} DONE", regionId);
	}

	/**
	 * Redistribute the given sstable
	 * @param ssTableName
	 * @param source 
	 * @param destination 
	 * @throws StorageManagerException 
	 */
	private void distributeData(final TupleStoreName ssTableName, final DistributionRegion source,
			final List<DistributionRegion> destination) 
			throws BBoxDBException, StorageManagerException {
		
		logger.info("Redistributing table {}", ssTableName.getFullname());
		
		final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);
		
		// Spread data
		final TupleRedistributor tupleRedistributor = getTupleRedistributor(source, destination, ssTableName);
		spreadTupleStores(ssTableManager, tupleRedistributor);			
		
		logger.info("Redistributing table {} is DONE", ssTableName.getFullname());
	}

	/**
	 * Stop the to disk flushing
	 * @param ssTableName
	 * @throws StorageManagerException
	 */
	private void stopFlushToDisk(final TupleStoreName ssTableName) throws StorageManagerException {
		final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);
		
		// Stop flush thread, so new data remains in memory
		ssTableManager.setToReadOnly();
	}

	/**
	 * Get a new instance of the tuple re-distributor
	 * @param region
	 * @param destination 
	 * @param ssTableName
	 * @return
	 * @throws StorageManagerException
	 */
	private TupleRedistributor getTupleRedistributor(final DistributionRegion region, 
			final List<DistributionRegion> destination, final TupleStoreName ssTableName)
			throws StorageManagerException {
		
		final TupleRedistributor tupleRedistributor = new TupleRedistributor(registry, ssTableName);
		
		for(final DistributionRegion childRegion : destination) {
			tupleRedistributor.registerRegion(childRegion);
		}
		
		return tupleRedistributor;
	}

	/**
	 * Spread a given tuple store onto new systems
	 * @param region
	 * @param sstableManager
	 * @param ssTableManager
	 * @param tupleRedistributor
	 * @param onlyInMemoryData 
	 * @throws StorageManagerException 
	 */
	private void spreadTupleStores(final TupleStoreManager ssTableManager, 
			final TupleRedistributor tupleRedistributor) throws BBoxDBException {
		
		final List<ReadOnlyTupleStore> storages = new ArrayList<>();
		
		try {
			final List<ReadOnlyTupleStore> aquiredStorages = ssTableManager.aquireStorage();
			storages.addAll(aquiredStorages);
			
			final int totalSotrages = aquiredStorages.size();
			
			for(int i = 0; i < totalSotrages; i++) {
				final ReadOnlyTupleStore storage = aquiredStorages.get(i);
				logger.info("Spread tuple storage {} number {} of {}", 
						storage.getInternalName(), i, totalSotrages - 1);
						spreadStorage(tupleRedistributor, storage);
			}

			logger.info("Final statistics for spread ({}): {}", 
					ssTableManager.getTupleStoreName().getFullname(),
					tupleRedistributor.getStatistics());
			
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} finally {
			ssTableManager.releaseStorage(storages);
		}
	}

	/**
	 * Spread the tuple storage
	 * @param tupleRedistributor
	 * @param storage
	 * @param distributeSuccessfully
	 * @return
	 * @throws Exception 
	 */
	private void spreadStorage(final TupleRedistributor tupleRedistributor,
			final ReadOnlyTupleStore storage) throws Exception {
		
		for(final Tuple tuple : storage) {
			tupleRedistributor.redistributeTuple(tuple);
		}		
	}
}

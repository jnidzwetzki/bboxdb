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
import java.util.Arrays;
import java.util.List;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.DistributionRegionIdMapperManager;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
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
		assert(region.isLeafRegion()) : "Unable to perform split on: " + region;
		
		final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getDistributionGroupAdapter();
		
		logger.info("Performing split for: {}", region.getIdentifier());
		
		final boolean setResult = tryToSetToFullSplitting(region, distributionGroupZookeeperAdapter);
		
		if(! setResult) {
			return;
		}
		
		boolean splitFailed = false;
		
		try {
			spacePartitioner.splitRegion(region, tupleStoreManagerRegistry);
		} catch (Throwable e) {
			logger.info("Finding split point failed, retry in a few minutes" + region.getIdentifier());
			splitFailed = true;
		}
		
		try {
			if(! splitFailed) {
				redistributeDataSplit(region);
				distributionGroupZookeeperAdapter.deleteRegionStatistics(region);
			}
		} catch (Throwable e) {
			logger.warn("Got uncought exception during split: " + region.getIdentifier(), e);
			splitFailed = true;
		}

		if(splitFailed) {
			resetAreaStateNE(region, distributionGroupZookeeperAdapter);
		}
		
		logger.info("Performing split for: {} is done", region.getIdentifier());
	}

	/**
	 * Reset the are state
	 * @param region
	 * @param distributionGroupZookeeperAdapter
	 */
	private void resetAreaStateNE(final DistributionRegion region,
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter) {
		
		try {
			distributionGroupZookeeperAdapter.setStateForDistributionRegion(region, DistributionRegionState.ACTIVE);
		} catch (ZookeeperException e) {
			logger.error("Got exception while resetting area state for to active, "
					+ "your global index might be inconsistent now "+ region.getIdentifier(), e);
		}
	}

	/**
	 * Try to set the region split state
	 * 
	 * @param region
	 * @param distributionGroupZookeeperAdapter
	 * @return 
	 */
	private boolean tryToSetToFullSplitting(final DistributionRegion region,
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter) {
		
		try {
			// Try to set region state to full. If this fails, another node is already 
			// splits the region
			final boolean setToFullResult = distributionGroupZookeeperAdapter.setToFull(region);
			
			if(! setToFullResult) {
				final DistributionRegionState stateForDistributionRegion 
					= distributionGroupZookeeperAdapter.getStateForDistributionRegion(region);
				
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
	 * Merge the given region
	 * @param region
	 * @param distributionGroupZookeeperAdapter
	 * @param spacePartitioner
	 * @param diskStorage
	 */
	public void mergeRegion(final DistributionRegion region, final SpacePartitioner spacePartitioner,
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) {
		
		assert(region != null);
		assert(! region.isLeafRegion()) : "Unable to perform merge on: " + region + " is leaf";

		logger.info("Performing merge for: {}", region.getIdentifier());
		
		final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();

		try {
			// Try to set region state to full. If this fails, another node is already 
			// splits the region
			final boolean setToMergeResult = distributionGroupZookeeperAdapter.setToSplitMerging(region);
			
			if(! setToMergeResult) {
				logger.info("Unable to set state to split merge for region: {}, stopping merge", region.getIdentifier());
				logger.info("Old state was {}", distributionGroupZookeeperAdapter.getStateForDistributionRegion(region));
				return;
			}
			
			spacePartitioner.prepareMerge(region);
			redistributeDataMerge(region);
			spacePartitioner.mergeComplete(region);

		} catch (Throwable e) {
			logger.warn("Got uncought exception during merge: " + region.getIdentifier(), e);
		}
	}

	/**
	 * Redistribute the data in region merge
	 * @param region
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	protected void redistributeDataMerge(final DistributionRegion region) 
			throws StorageManagerException, BBoxDBException {
		
		logger.info("Redistributing all data for region (merge): " + region.getIdentifier());

		final List<DistributionRegion> childRegions = Arrays.asList(region.getLeftChild(), 
				region.getRightChild());
		
		final DistributionGroupName distributionGroupName = region.getDistributionGroupName();
		
		final List<TupleStoreName> localTables = registry.getAllTablesForDistributionGroupAndRegionId
				(distributionGroupName, region.getRegionId());

		// Remove the local mapping, no new data is written to the region
		final DistributionRegionIdMapper mapper = DistributionRegionIdMapperManager.getInstance(distributionGroupName);
		final boolean addResult = mapper.addMapping(region);
		
		assert (addResult == true) : "Unable to add mapping for: " + region;
		
		// Redistribute data
		for(final TupleStoreName tupleStoreName : localTables) {
			logger.info("Merging data of tuple store {}", tupleStoreName);
			startFlushToDisk(tupleStoreName);
			
			final TupleRedistributor tupleRedistributor 
				= new TupleRedistributor(registry, tupleStoreName);
			
			tupleRedistributor.registerRegion(region);
			
			for(final DistributionRegion childRegion : childRegions) {
				mergeDataFromChildRegion(region, tupleStoreName, tupleRedistributor, childRegion);					
			}
		}
	}

	/**
	 * Merge the data from the given child region
	 * @param region
	 * @param tupleStoreName
	 * @param tupleRedistributor
	 * @param childRegion
	 * @throws StorageManagerException
	 */
	private void mergeDataFromChildRegion(final DistributionRegion region, 
			final TupleStoreName tupleStoreName,	final TupleRedistributor tupleRedistributor, 
			final DistributionRegion childRegion) throws StorageManagerException {
		
		try {
			final List<BBoxDBInstance> systems = region.getSystems();
			assert(! systems.isEmpty()) : "Systems can not be empty";
			
			final BBoxDBInstance firstSystem = systems.get(0);
			
			final BBoxDBClient connection = MembershipConnectionService.getInstance()
					.getConnectionForInstance(firstSystem);
			
			assert (connection != null) : "Connection can not be null: " + firstSystem.getStringValue();
			
			final BoundingBox bbox = childRegion.getConveringBox();
			final String fullname = tupleStoreName.getFullname();
			final TupleListFuture result = connection.queryBoundingBox(fullname, bbox);
			
			result.waitForAll();
			
			if(result.isFailed()) {
				throw new StorageManagerException("Exception while fetching tuples: " 
						+ result.getAllMessages());
			}
			
			for(final Tuple tuple : result) {
				tupleRedistributor.redistributeTuple(tuple);
			}
			
			logger.info("Final statistics for merge ({}): {}", 
					fullname,
					tupleRedistributor.getStatistics());
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new StorageManagerException(e);
		} catch (Exception e) {
			throw new StorageManagerException(e);
		}
	}
	
	/**
	 * Redistribute data after region split
	 * @param region
	 */
	protected void redistributeDataSplit(final DistributionRegion region) {
		try {
			logger.info("Redistributing all data for region: " + region.getIdentifier());
			
			assertChildIsReady(region);
			
			final DistributionGroupName distributionGroupName = region.getDistributionGroupName();
			final long regionId = region.getRegionId();
			
			final List<TupleStoreName> localTables = registry
					.getAllTablesForDistributionGroupAndRegionId(distributionGroupName, regionId);
	
			// Remove the local mapping, no new data is written to the region
			final DistributionRegionIdMapper mapper = DistributionRegionIdMapperManager.getInstance(distributionGroupName);
			final boolean removeResult = mapper.removeMapping(regionId);
			
			assert (removeResult == true) : "Unable to remove mapping for: " + region;
			
			// Redistribute data
			for(final TupleStoreName ssTableName : localTables) {
				// Reject new writes and flush to disk
				stopFlushToDisk(ssTableName);
				distributeData(ssTableName, region);	
			}
			
			// Update zookeeer
			final DistributionGroupZookeeperAdapter zookeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			zookeperAdapter.setStateForDistributionRegion(region, DistributionRegionState.SPLIT);

			// Remove local data
			logger.info("Deleting local data for {}", region.getIdentifier());
			deleteLocalData(localTables);
		} catch (InterruptedException e) {
			logger.warn("Thread was interrupted");
			Thread.currentThread().interrupt();
			return;
		} catch (Exception e) {
			logger.error("Got exception when deleting local data", e);
			return;
		}
		
		logger.info("Redistributing data for region: {} DONE", region.getIdentifier());
	}

	/**
	 * Delete the local data for the given sstables
	 * @param localTables
	 * @throws StorageManagerException
	 * @throws Exception
	 * @throws InterruptedException
	 */
	protected void deleteLocalData(final List<TupleStoreName> localTables)
			throws StorageManagerException, Exception, InterruptedException {
		
		for(final TupleStoreName ssTableName : localTables) {
			final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);
			
			final List<ReadOnlyTupleStore> storages = new ArrayList<>();
			
			try {
				final List<ReadOnlyTupleStore> aquiredStorages = ssTableManager.aquireStorage();
				storages.addAll(aquiredStorages);
				storages.forEach(s -> s.deleteOnClose());	
			} catch (Exception e) {
				throw e;
			} finally {
				ssTableManager.releaseStorage(storages);
			}
			
			ssTableManager.shutdown();
			ssTableManager.awaitShutdown();
		}
	}

	/**
	 * Assert child is ready
	 * @param region
	 */
	protected void assertChildIsReady(final DistributionRegion region) {
		
		final DistributionRegion leftChild = region.getLeftChild();
		final DistributionRegion rightChild = region.getRightChild();
		
		assert (! region.isLeafRegion()) : "Region " + region.getIdentifier() 
			+ " is a leaf region. Left child: " + leftChild + " right child: " 
			+ rightChild;
		
		assert (! leftChild.getSystems().isEmpty()) : "Region " +  leftChild.getIdentifier() 
			+ " state " +  leftChild.getState() + " systems " + leftChild.getSystems();
		
		assert (! rightChild.getSystems().isEmpty()) : "Region " +  rightChild.getIdentifier() 
		+ " state " +  rightChild.getState() + " systems " + rightChild.getSystems();
	}

	/**
	 * Redistribute the given sstable
	 * @param ssTableName
	 * @param region 
	 * @throws StorageManagerException 
	 */
	protected void distributeData(final TupleStoreName ssTableName, final DistributionRegion region) 
			throws BBoxDBException, StorageManagerException {
		
		logger.info("Redistributing table {}", ssTableName.getFullname());
		
		final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);
		
		// Spread data
		final TupleRedistributor tupleRedistributor = getTupleRedistributor(region, ssTableName);
		spreadTupleStores(ssTableManager, tupleRedistributor);			
		
		logger.info("Redistributing table {} is DONE", ssTableName.getFullname());
	}

	/**
	 * Stop the to disk flushing
	 * @param ssTableName
	 * @throws StorageManagerException
	 */
	protected void stopFlushToDisk(final TupleStoreName ssTableName) throws StorageManagerException {
		final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);
		
		// Stop flush thread, so new data remains in memory
		ssTableManager.setToReadOnly();
	}
	
	/**
	 * Start the to disk flushing
	 * @param ssTableName
	 * @throws StorageManagerException
	 */
	protected void startFlushToDisk(final TupleStoreName ssTableName) throws StorageManagerException {
		final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);		
		ssTableManager.setToReadWrite();
	}

	/**
	 * Get a new instance of the tuple redistributor
	 * @param region
	 * @param ssTableName
	 * @return
	 * @throws StorageManagerException
	 */
	protected TupleRedistributor getTupleRedistributor(final DistributionRegion region, final TupleStoreName ssTableName)
			throws StorageManagerException {
		
		final DistributionRegion leftRegion = region.getLeftChild();
		final DistributionRegion rightRegion = region.getRightChild();
		
		final TupleRedistributor tupleRedistributor = new TupleRedistributor(registry, ssTableName);
		tupleRedistributor.registerRegion(leftRegion);
		tupleRedistributor.registerRegion(rightRegion);
		
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
	protected void spreadTupleStores(final TupleStoreManager ssTableManager, 
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
	protected void spreadStorage(final TupleRedistributor tupleRedistributor,
			final ReadOnlyTupleStore storage) throws Exception {
		
		for(final Tuple tuple : storage) {
			tupleRedistributor.redistributeTuple(tuple);
		}		
	}
}

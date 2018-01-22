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
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitter {

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The tree adapter
	 */
	protected SpacePartitioner spacePartitioner = null;
	
	/**
	 * The distribution group adapter 
	 */
	protected DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = null;

	/**
	 * The region to split;
	 */
	protected DistributionRegion region = null;
	
	/**
	 * The storage reference
	 */
	protected DiskStorage storage;

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionSplitter.class);
	
	public RegionSplitter() {
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		this.distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	}
	
	/**
	 * Perform a distribution region split
	 * @param region
	 * @param distributionGroupZookeeperAdapter
	 * @param spacePartitioner
	 * @param diskStorage
	 */
	public void splitRegion(final DistributionRegion region, 
			final SpacePartitioner spacePartitioner, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) {
		
		assert(region != null);
		assert(region.isLeafRegion()) : "Unable to perform split on: " + region;
		
		final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getDistributionGroupAdapter();
		
		logger.info("Performing split for: {}", region.getIdentifier());
		
		try {
			// Try to set region state to full. If this fails, another node is already 
			// splits the region
			final boolean setToFullResult = distributionGroupZookeeperAdapter.setToFull(region);
			
			if(! setToFullResult) {
				logger.info("Unable to set state to full for region: {}, stopping split", region.getIdentifier());
				logger.info("Old state was {}", distributionGroupZookeeperAdapter.getStateForDistributionRegion(region));
				return;
			}
			
			spacePartitioner.splitRegion(region, tupleStoreManagerRegistry);
			redistributeDataSplit(region);
		} catch (Throwable e) {
			logger.warn("Got uncought exception during split: " + region.getIdentifier(), e);
		}

		logger.info("Performing split for: {} is done", region.getIdentifier());
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
			
		} catch (Throwable e) {
			logger.warn("Got uncought exception during merge: " + region.getIdentifier(), e);
		}
	}

	/**
	 * Set the distribution region
	 * @param region
	 * @throws StorageManagerException 
	 */
	public void initFromSSTablename(final DiskStorage storage, final TupleStoreName ssTableName) throws StorageManagerException {
		
		assert (spacePartitioner == null) : "Unable to reinit instance";
		assert (region == null) : "Unable to reinit instance";

		try {
			this.storage = storage;
			
			spacePartitioner = SpacePartitionerCache.getSpacePartitionerForGroupName(
					ssTableName.getDistributionGroup());

			final DistributionRegion distributionGroup = spacePartitioner.getRootNode();
			
			final long nameprefix = ssTableName.getRegionId();
			
			region = DistributionRegionHelper.getDistributionRegionForNamePrefix(
					distributionGroup, nameprefix);
			
			if(region == null) {
				throw new StorageManagerException("Region for nameprefix " + nameprefix + " is not found");
			}
			
			if(! region.isLeafRegion()) {
				throw new StorageManagerException("Region is not a leaf region, unable to split:" 
						+ region.getIdentifier());
			}
		} catch (ZookeeperException e) {
			logger.error("Got exception while init region splitter", e);
			region = null;
			throw new StorageManagerException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
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
		
		final List<TupleStoreName> localTables = storage.getTupleStoreManagerRegistry()
				.getAllTablesForDistributionGroupAndRegionId
				(distributionGroupName, region.getRegionId());

		// Remove the local mapping, no new data is written to the region
		final RegionIdMapper mapper = RegionIdMapperInstanceManager.getInstance(distributionGroupName);
		final boolean addResult = mapper.addMapping(region);
		
		assert (addResult == true) : "Unable to add mapping for: " + region;
		
		// Redistribute data
		for(final TupleStoreName tupleStoreName : localTables) {
			logger.info("Merging data of tuple store {}", tupleStoreName);
			startFlushToDisk(tupleStoreName);
			
			final TupleRedistributor tupleRedistributor 
				= new TupleRedistributor(storage.getTupleStoreManagerRegistry(), tupleStoreName);
			tupleRedistributor.registerRegion(region);
			
			for(final DistributionRegion childRegion : childRegions) {
				mergeDataFromChildRegion(region, tupleStoreName, tupleRedistributor, childRegion);					
			}

			spacePartitioner.mergeComplete(region);
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
			final BoundingBox bbox = childRegion.getConveringBox();
			
			final BBoxDBClient connection = MembershipConnectionService.getInstance()
					.getConnectionForInstance(region.getSystems().iterator().next());
			
			final TupleListFuture result = connection.queryBoundingBox(tupleStoreName.getFullname(),
					bbox);
			
			result.waitForAll();
			
			if(result.isFailed()) {
				throw new StorageManagerException("Exception while fetching tuples: " 
						+ result.getAllMessages());
			}
			
			for(final Tuple tuple : result) {
				tupleRedistributor.redistributeTuple(tuple);
			}
			
			logger.info("Final statistics for merge ({}): {}", 
					tupleStoreName.getFullname(),
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
			
			final List<TupleStoreName> localTables = storage.getTupleStoreManagerRegistry()
					.getAllTablesForDistributionGroupAndRegionId
					(distributionGroupName, region.getRegionId());
	
			// Remove the local mapping, no new data is written to the region
			final RegionIdMapper mapper = RegionIdMapperInstanceManager.getInstance(distributionGroupName);
			final boolean removeResult = mapper.removeMapping(region.getRegionId());
			
			assert (removeResult == true) : "Unable to remove mapping for: " + region;
			
			// Redistribute data
			for(final TupleStoreName ssTableName : localTables) {
				// Reject new writes and flush to disk
				stopFlushToDisk(ssTableName);
				distributeData(ssTableName);	
			}
			
			// Update zookeeer
			final DistributionGroupZookeeperAdapter zookeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			zookeperAdapter.setStateForDistributionGroup(region, DistributionRegionState.SPLIT);

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
			final TupleStoreManager ssTableManager = storage.getTupleStoreManagerRegistry().getTupleStoreManager(ssTableName);
			
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
	 * @param region
	 * @param ssTableName
	 * @throws StorageManagerException 
	 */
	protected void distributeData(final TupleStoreName ssTableName) 
			throws BBoxDBException, StorageManagerException {
		
		logger.info("Redistributing table {}", ssTableName.getFullname());
		
		final TupleStoreManager ssTableManager = storage.getTupleStoreManagerRegistry().getTupleStoreManager(ssTableName);
		
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
		final TupleStoreManager ssTableManager = storage.getTupleStoreManagerRegistry().getTupleStoreManager(ssTableName);
		
		// Stop flush thread, so new data remains in memory
		ssTableManager.setToReadOnly();
	}
	
	/**
	 * Start the to disk flushing
	 * @param ssTableName
	 * @throws StorageManagerException
	 */
	protected void startFlushToDisk(final TupleStoreName ssTableName) throws StorageManagerException {
		final TupleStoreManager ssTableManager = storage.getTupleStoreManagerRegistry().getTupleStoreManager(ssTableName);		
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
		
		final TupleRedistributor tupleRedistributor = new TupleRedistributor(
				storage.getTupleStoreManagerRegistry(), ssTableName);
		
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
				logger.info("Spread sstable facade {} number {} of {}", 
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

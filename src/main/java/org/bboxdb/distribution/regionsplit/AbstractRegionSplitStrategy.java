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
package org.bboxdb.distribution.regionsplit;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.DistributionRegionState;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableFlushMode;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRegionSplitStrategy implements Runnable {
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The tree adapter
	 */
	protected KDtreeZookeeperAdapter treeAdapter = null;
	
	/**
	 * The distribution group adapter 
	 */
	protected DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = null;

	/**
	 * The region to split;
	 */
	protected DistributionRegion region = null;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(AbstractRegionSplitStrategy.class);

	public AbstractRegionSplitStrategy() {
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		this.distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	}
		
	/**
	 * Set the distribution region
	 * @param region
	 * @throws StorageManagerException 
	 */
	public void initFromSSTablename(final SSTableName ssTableName) throws StorageManagerException {
		
		assert (treeAdapter == null) : "Unable to reinit instance";
		assert (region == null) : "Unable to reinit instance";

		try {
			treeAdapter = DistributionGroupCache.getGroupForGroupName(
					ssTableName.getDistributionGroup(), zookeeperClient);

			final DistributionRegion distributionGroup = treeAdapter.getRootNode();
			
			final int nameprefix = ssTableName.getRegionId();
			
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
		}
	}
	
	/**
	 * Is a split needed?
	 * @param totalTuplesInTable
	 * @return
	 */
	public boolean isSplitNeeded(final long sizeOfRegion) {
		
		// Is the data of the parent completely distributed?
		if(! isParentDataRedistributed()) {
			return false;
		}
		
		final long maxSize = getRegionMaxSize();
		
		return (sizeOfRegion > maxSize);
	}
	
	/**
	 * Is the data of the region parent completely redistributed, 
	 * if not, wait with local split
	 * @return
	 */
	protected boolean isParentDataRedistributed() {
		
		// Root region
		if(region.getParent() == DistributionRegion.ROOT_NODE_ROOT_POINTER) {
			return true;
		}
		
		return region.getParent().getState() == DistributionRegionState.SPLIT;
	}

	/**
	 * Get maximal size of a region
	 * @return
	 */
	protected long getRegionMaxSize() {
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		return configuration.getRegionMaxSize();
	}
	
	/**
	 * Perform a split of the given region
	 * @param region
	 * @return 
	 */
	protected abstract boolean performSplit(final DistributionRegion region);

	/**
	 * Perform a SSTable split
	 * @param sstableManager
	 * @return Split performed or not
	 */
	public void run() {
		
		assert(region != null);
		assert(region.isLeafRegion()) : "Unable to perform split on: " + region;
		
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
			
			final boolean splitResult = performSplit(region);
			
			if(splitResult == false) {
				logger.error("Unable to split region {}, stopping split!", region.getIdentifier());
			} else {
				redistributeData(region);
			}
		} catch (Throwable e) {
			logger.warn("Got uncought exception during split: " + region.getIdentifier(), e);
		}

		logger.info("Performing split for: {} is done", region.getIdentifier());
	}

	/**
	 * Redistribute data after region split
	 * @param region
	 */
	protected void redistributeData(final DistributionRegion region) {
		try {
			logger.info("Redistributing all data for region: " + region.getIdentifier());
			
			assertChildIsReady(region);
			
			final DistributionGroupName distributionGroupName = region.getDistributionGroupName();
			
			final List<SSTableName> localTables = StorageRegistry.getInstance()
					.getAllTablesForDistributionGroupAndRegionId
					(distributionGroupName, region.getRegionId());
	
			// Redistribute all data, new data is kept in memory
			for(final SSTableName ssTableName : localTables) {
				stopFlushToDisk(ssTableName);
				distributeData(region, ssTableName, false);	
			}
			
			// Remove the local mapping, no new data is written to the region
			final RegionIdMapper mapper = RegionIdMapperInstanceManager.getInstance(distributionGroupName);
			final boolean removeResult = mapper.removeMapping(region.getRegionId());
			
			assert (removeResult == true) : "Unable to remove mapping for: " + region;
			
			// Redistribute only in memory data
			logger.info("Redistributing in-memory data for region: {}", region.getIdentifier());
			for(final SSTableName ssTableName : localTables) {	
				distributeData(region, ssTableName, true);	
			}
			
			// Update zookeeer
			final DistributionGroupZookeeperAdapter zookeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			zookeperAdapter.setStateForDistributionGroup(region, DistributionRegionState.SPLIT);

			// Remove local data
			logger.info("Deleting local data for {}", region.getIdentifier());
			deleteLocalData(localTables);
		} catch (ZookeeperException e) {
			logger.error("Got an exception while setting region state to splitted", e);
		}  catch (InterruptedException e) {
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
	protected void deleteLocalData(final List<SSTableName> localTables)
			throws StorageManagerException, Exception, InterruptedException {
		
		for(final SSTableName ssTableName : localTables) {
			final SSTableManager ssTableManager = StorageRegistry.getInstance().getSSTableManager(ssTableName);
			
			final List<ReadOnlyTupleStorage> storages = new ArrayList<>();
			
			try {
				final List<ReadOnlyTupleStorage> aquiredStorages = ssTableManager.aquireStorage();
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
	protected void distributeData(final DistributionRegion region, 
			final SSTableName ssTableName, final boolean onlyInMemoryData) throws Exception {
		
		logger.info("Redistributing table {}", ssTableName.getFullname());
		
		final SSTableManager ssTableManager = StorageRegistry.getInstance().getSSTableManager(ssTableName);
		
		// Spread data
		final TupleRedistributor tupleRedistributor = getTupleRedistributor(region, ssTableName);
		spreadTupleStores(region, ssTableManager, tupleRedistributor, onlyInMemoryData);			
		
		logger.info("Redistributing table " + ssTableName.getFullname() + " is DONE");
	}

	/**
	 * Stop the to disk flushing
	 * @param ssTableName
	 * @throws StorageManagerException
	 */
	protected void stopFlushToDisk(final SSTableName ssTableName) throws StorageManagerException {
		final SSTableManager ssTableManager = StorageRegistry.getInstance().getSSTableManager(ssTableName);
		
		// FLush data to disk
		ssTableManager.flush();
		ssTableManager.setSSTableFlushMode(SSTableFlushMode.MEMORY_ONLY);
		
		// Stop flush thread, so new data remains in memory
		ssTableManager.stopThreads();
	}

	/**
	 * Get a new instance of the tuple redistributor
	 * @param region
	 * @param ssTableName
	 * @return
	 * @throws StorageManagerException
	 */
	protected TupleRedistributor getTupleRedistributor(final DistributionRegion region, final SSTableName ssTableName)
			throws StorageManagerException {
		
		final DistributionRegion leftRegion = region.getLeftChild();
		final DistributionRegion rightRegion = region.getRightChild();
		
		final TupleRedistributor tupleRedistributor = new TupleRedistributor(ssTableName);
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
	protected void spreadTupleStores(final DistributionRegion region,
			final SSTableManager ssTableManager, final TupleRedistributor tupleRedistributor, 
			final boolean onlyInMemoryData) throws Exception {
		
		final List<ReadOnlyTupleStorage> storages = new ArrayList<>();
		
		try {
			final List<ReadOnlyTupleStorage> aquiredStorages = ssTableManager.aquireStorage();
			storages.addAll(aquiredStorages);
			
			for(final ReadOnlyTupleStorage storage: storages) {
				// Skip persistent data, if needed
				if(onlyInMemoryData && storage.isPersistent()) {
					continue;
				}
				
				logger.info("Spread sstable facade: {}", storage.getInternalName());
				spreadStorage(tupleRedistributor, storage);
			}				
			
			logger.info("Statistics for spread: {}", tupleRedistributor.getStatistics());
		} catch (Exception e) {
			throw e;
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
			final ReadOnlyTupleStorage storage) throws Exception {
		
		for(final Tuple tuple : storage) {
			tupleRedistributor.redistributeTuple(tuple);
		}		
	}

	/**
	 * Perform a split at the given position
	 * @param region
	 * @param splitPosition
	 */
	protected void performSplitAtPosition(final DistributionRegion region, final double splitPosition) {
		try {
			logger.info("Set split for {} at: {}", region.getIdentifier(), splitPosition);
			treeAdapter.splitNode(region, splitPosition);
			assertChildIsReady(region);
		} catch (ZookeeperException | ResourceAllocationException e) {
			logger.warn("Unable to split region " + region.getIdentifier() + " at " + splitPosition, e);
		} 
	}
}

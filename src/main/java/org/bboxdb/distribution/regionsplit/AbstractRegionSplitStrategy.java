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

import java.util.Collection;
import java.util.List;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.DistributionRegionState;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
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
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
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
	public boolean isSplitNeeded(final int totalTuplesInTable) {
		
		if(! isParentDataRedistributed()) {
			return false;
		}
		
		final int maxEntries = maxEntriesPerTable();
		
		if(totalTuplesInTable > maxEntries) {
			return true;
		} else {
			return false;
		}
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
		
		return region.getParent().getState() == DistributionRegionState.SPLITTED;
	}

	/**
	 * Get the number of entries per table
	 * @return
	 */
	protected int maxEntriesPerTable() {
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		return configuration.getSstableMaxEntries();
	}
	
	/**
	 * Perform a split of the given region
	 * @param region
	 * @return 
	 */
	protected abstract boolean performSplit(final DistributionRegion region);

	/**
	 * Perform a SSTable split
	 * @param ssTableName
	 * @return Split performed or not
	 */
	public void run() {
		
		assert(region != null);
		assert(region.isLeafRegion());
		
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
		logger.info("Redistributing data for region: " + region.getIdentifier());
		
		assert (! region.isLeafRegion()) : "Region " + region.getIdentifier() 
			+ " is a leaf region. Left child: " + region.getLeftChild() + " right child: " 
			+ region.getRightChild();
		
		final List<SSTableName> localTables = StorageRegistry.getAllTablesForDistributionGroupAndRegionId
				(region.getDistributionGroupName(), region.getRegionId());
		
		for(final SSTableName ssTableName : localTables) {
			if(ssTableName.getDistributionGroupObject().equals(region.getDistributionGroupName())) {
				redistributeTable(region, ssTableName);
			}
		}
		
		try {
			final DistributionGroupZookeeperAdapter zookeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			zookeperAdapter.setStateForDistributionGroup(region, DistributionRegionState.SPLITTED);
		} catch (ZookeeperException e) {
			logger.error("Got an exception while setting region state to splitted", e);
		}
		
		logger.info("Redistributing data for region: " + region.getIdentifier() + " DONE");
	}

	/**
	 * Redistribute the given sstable
	 * @param region
	 * @param ssTableName
	 */
	protected void redistributeTable(final DistributionRegion region, final SSTableName ssTableName) {
		logger.info("Redistributing table {}", ssTableName.getFullname());
		
		try {
			final SSTableManager ssTableManager = StorageRegistry.getSSTableManager(ssTableName);
			
			// Stop flush thread, so new data remains in memory
			ssTableManager.stopThreads();
			
			// Spread on disk data
			spreadPersistantStorages(region, ssTableName, ssTableManager);
			
		} catch (StorageManagerException e) {
			logger.warn("Got an exception while distributing tuples for: " + ssTableName, e);
		}
		
		logger.info("Redistributing table " + ssTableName.getFullname() + " is DONE");
	}

	/**
	 * Spread the given storages
	 * @param region
	 * @param ssTableName
	 * @param ssTableManager 
	 * @param storages
	 */
	protected void spreadPersistantStorages(final DistributionRegion region, 
			final SSTableName ssTableName, final SSTableManager ssTableManager) {

		assert (! region.isLeafRegion());
		final DistributionRegion leftRegion = region.getLeftChild();
		final DistributionRegion rightRegion = region.getRightChild();
		
		try {
			final TupleRedistributor tupleRedistributor = new TupleRedistributor(ssTableName);
			tupleRedistributor.registerRegion(leftRegion);
			tupleRedistributor.registerRegion(rightRegion);
			
			spreadTupleStores(region, ssTableName, ssTableManager, tupleRedistributor);
		} catch (StorageManagerException e) {
			logger.error("Got an exception while redistributing tuples");
		}

	}

	/**
	 * Spread a given tuple store onto new systems
	 * @param region
	 * @param ssTableName
	 * @param ssTableManager
	 * @param tupleRedistributor
	 */
	protected void spreadTupleStores(final DistributionRegion region,
			final SSTableName ssTableName, final SSTableManager ssTableManager, 
			final TupleRedistributor tupleRedistributor) {
		
		final Collection<ReadOnlyTupleStorage> storages = ssTableManager.getTupleStoreInstances().getAllTupleStorages();
		
		for(final ReadOnlyTupleStorage storage: storages) {
			final boolean acquired = storage.acquire();
			
			if(acquired) {	
				logger.info("Spread sstable facade: {}", storage.getInternalName());
				boolean distributeSuccessfully = true;
				
				for(final Tuple tuple : storage) {
					try {
						tupleRedistributor.redistributeTuple(tuple);
					} catch (Exception e) {
						logger.error("Got exeption while redistributing tuples", e);
						distributeSuccessfully = false;
					}
				}
				
				// Data is spread, so we can delete it
				if(! distributeSuccessfully) {
					logger.warn("Distribution of {} was not successfully", storage.getInternalName());
				} else {
					logger.info("Distribution of {} was successfully, scheduling for deletion", storage.getInternalName());
					storage.deleteOnClose();
				}
				
				logger.info("Statistics for spread: " + tupleRedistributor.getStatistics());
				
				storage.release();
			}
		}
	}

	/**
	 * Perform a split at the given position
	 * @param region
	 * @param splitPosition
	 */
	protected void performSplitAtPosition(final DistributionRegion region, final float splitPosition) {
		try {
			logger.info("Set split for {} at: {}", region.getIdentifier(), splitPosition);
			treeAdapter.splitNode(region, splitPosition);
			
			assert (! region.isLeafRegion()) : "Region " + region.getIdentifier() + " is a leaf region after split";

		} catch (ZookeeperException | ResourceAllocationException e) {
			logger.warn("Unable to split region " + region.getIdentifier() + " at " + splitPosition, e);
		} 
	}

}

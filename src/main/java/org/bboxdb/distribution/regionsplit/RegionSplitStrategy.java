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
package org.bboxdb.distribution.regionsplit;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.mode.DistributionRegionState;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RegionSplitStrategy implements Runnable {
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	private KDtreeZookeeperAdapter distributionAdapter;

	/**
	 * The region to split;
	 */
	protected DistributionRegion region;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionSplitStrategy.class);



	public RegionSplitStrategy() {
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
	}
		
	/**
	 * Set the distribution region
	 * @param region
	 * @throws StorageManagerException 
	 */
	public void initFromSSTablename(final SSTableName ssTableName) throws StorageManagerException {
		
		try {
			distributionAdapter = DistributionGroupCache.getGroupForGroupName(
					ssTableName.getDistributionGroup(), zookeeperClient);

			final DistributionRegion distributionGroup = distributionAdapter.getRootNode();
			
			region = DistributionRegionHelper.getDistributionRegionForNamePrefix(
					distributionGroup, ssTableName.getNameprefix());
			
			if(region == null) {
				throw new StorageManagerException("Region is null");
			}
			
			if(! region.isLeafRegion()) {
				throw new StorageManagerException("Region is not a leaf region, unable to split:" + region);
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
		final int maxEntries = maxEntriesPerTable();
		
		if(totalTuplesInTable > maxEntries) {
			return true;
		} else {
			return false;
		}
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
	 */
	protected abstract void performSplit(final DistributionRegion region);

	/**
	 * Perform a SSTable split
	 * @param ssTableName
	 * @return Split performed or not
	 */
	public void run() {
		
		if(region == null) {
			logger.error("Unable to perform split, region is not set");
			return;
		}
		
		logger.info("Performing split for: {}", region);
		
		try {
			performSplit(region);
			redistributeData(region);
		} catch (Throwable e) {
			logger.warn("Got uncought exception during split: " + region, e);
		}

		logger.info("Performing split for: {} is done", region);
	}

	/**
	 * Redistribute data after region split
	 * @param region
	 */
	protected void redistributeData(final DistributionRegion region) {
		logger.info("Redistributing data for region: " + region);
		
		final List<SSTableName> localTables = StorageRegistry.getAllTables();
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
		
		logger.info("Redistributing data for region: " + region + " DONE");
	}

	/**
	 * Redistribute the given sstable
	 * @param region
	 * @param ssTableName
	 */
	protected void redistributeTable(final DistributionRegion region, final SSTableName ssTableName) {
		logger.info("Redistributing table " + ssTableName.getFullname());
		
		try {
			final SSTableManager ssTableManager = StorageRegistry.getSSTableManager(ssTableName);
			
			// Stop flush thread, so new data remains in memory
			ssTableManager.stopThreads();
			
			// Spread on disk data
			final Collection<ReadOnlyTupleStorage> storages = ssTableManager.getTupleStoreInstances().getAllTupleStorages();
			spreadStorages(region, ssTableName, storages);
			
		} catch (StorageManagerException e) {
			logger.warn("Got an exception while distributing tuples for: " + ssTableName, e);
		}
		
		logger.info("Redistributing table " + ssTableName.getFullname() + " is DONE");
	}

	/**
	 * Spread the given storages
	 * @param region
	 * @param ssTableName
	 * @param storages
	 */
	protected void spreadStorages(final DistributionRegion region, final SSTableName ssTableName, final Collection<ReadOnlyTupleStorage> storages) {

		for(final ReadOnlyTupleStorage storage: storages) {
			final boolean acquired = storage.acquire();
			
			if(acquired) {	
				logger.info("Spread sstable facade: {}", storage.getName());
				
				final boolean distributeSuccessfully 
					= spreadTuplesFromIterator(region, ssTableName, storage.iterator());
				
				// Data is spread, so we can delete it
				if(! distributeSuccessfully) {
					logger.warn("Distribution of {} was not successfully", storage.getName());
				} else {
					logger.info("Distribution of {} was successfully, scheduling for deletion", storage.getName());
					storage.deleteOnClose();
				}
				
				storage.release();
			}
		}
	}

	/**
	 * Spread the tuples from the given iterator
	 * @param region
	 * @param ssTableName
	 * @param tupleIterator
	 * @return 
	 */
	public boolean spreadTuplesFromIterator(final DistributionRegion region, final SSTableName ssTableName, final Iterator<Tuple> tupleIterator) {
		
		final DistributionRegion leftRegion = region.getLeftChild();
		final DistributionRegion rightRegion = region.getRightChild();
		
		final String tablename = ssTableName.getFullnameWithoutPrefix();
		
		final SSTableName leftSStablename = new SSTableName(ssTableName.getDimension(), 
				ssTableName.getDistributionGroup(), ssTableName.getTablename(), 
				leftRegion.getNameprefix());
		
		final SSTableName rightSStablename = new SSTableName(ssTableName.getDimension(), 
				ssTableName.getDistributionGroup(), ssTableName.getTablename(), 
				rightRegion.getNameprefix());
		
		boolean redistributeSuccessfully = true;
		
		while(tupleIterator.hasNext()) {
			
			final Tuple tuple = tupleIterator.next();
			
			// Spread to the left region
			if(tuple.getBoundingBox().overlaps(leftRegion.getConveringBox())) {
				
				final boolean tupleDistribute 
					= spreadTupleToSystems(tablename, leftSStablename, tuple, leftRegion);
				
				if(tupleDistribute == false) {
					redistributeSuccessfully = false;
				}
			}
			
			// Spread to the right region
			if(tuple.getBoundingBox().overlaps(rightRegion.getConveringBox())) {
				final boolean tupleDistribute 
					= spreadTupleToSystems(tablename, rightSStablename, tuple, rightRegion);
				
				if(tupleDistribute == false) {
					redistributeSuccessfully = false;
				}
			}
		}
		
		return redistributeSuccessfully;
	}
	
	/**
	 * Spread the tuple to the given list of systems
	 * @param ssTableName
	 * @param tuple
	 * @param region
	 * @return 
	 */
	protected boolean spreadTupleToSystems(final String baseTableName, final SSTableName ssTableName, final Tuple tuple, final DistributionRegion region) {
		
		final Collection<DistributedInstance> instances = region.getSystems();

		final MembershipConnectionService membershipConnectionService 	
			= MembershipConnectionService.getInstance();
		
		for(final DistributedInstance instance : instances) {
			
			// Redistribute tuples locally or via network?
			if(instance.socketAddressEquals(zookeeperClient.getInstancename())) {
				try {
					final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
					storageManager.put(tuple);
				} catch (StorageManagerException e) {
					logger.error("Got exception while inserting tuple", e);
					return false;
				}
			} else {
				final BBoxDBClient connection = membershipConnectionService.getConnectionForInstance(instance);
				
				if(connection == null) {
					logger.error("No connection for distributed instance {} is known, unable to distribute tuple.", instance);
					return false;
				}
				
				connection.insertTuple(baseTableName, tuple);
			}
		}
		
		return true;
	}

	/**
	 * Perform a split at the given position
	 * @param region
	 * @param splitPosition
	 */
	protected void performSplitAtPosition(final DistributionRegion region, final float splitPosition) {

		try {
			logger.info("Set split at:" + splitPosition);
			distributionAdapter.splitNode(region, splitPosition);
		
			// Allocate systems 
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			DistributionRegionHelper.allocateSystemsToNewRegion(region.getLeftChild(), zookeeperClient);
			DistributionRegionHelper.allocateSystemsToNewRegion(region.getRightChild(), zookeeperClient);
		
			// Let the data settle down
			Thread.sleep(5000);
			
		} catch (ZookeeperException | ResourceAllocationException e) {
			logger.warn("Unable to split region " + region + " at " + splitPosition, e);
		} catch (InterruptedException e) {
			logger.warn("Got InterruptedException while wait for settle down: " + region, e);
			Thread.currentThread().interrupt();
		}
	}

}

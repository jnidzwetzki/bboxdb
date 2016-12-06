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
package de.fernunihagen.dna.scalephant.distribution.regionsplit;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegionHelper;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.scalephant.distribution.placement.ResourceAllocationException;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.scalephant.storage.Memtable;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.StorageRegistry;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableKeyIndexReader;

public abstract class RegionSplitStrategy implements Runnable {
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;

	/**
	 * The region to split;
	 */
	protected DistributionRegion region;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionSplitStrategy.class);

	public RegionSplitStrategy() {
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
	}
		
	/**
	 * Set the distribution region
	 * @param region
	 */
	public void initFromSSTablename(final SSTableName ssTableName) {
		
		try {
			final DistributionRegion distributionGroup = DistributionGroupCache.getGroupForGroupName(
					ssTableName.getDistributionGroup(), zookeeperClient);
			
			final DistributionRegion region = DistributionRegionHelper.getDistributionRegionForNamePrefix(
					distributionGroup, ssTableName.getNameprefix());
			
			if(region == null) {
				throw new IllegalArgumentException("Region is null");
			}
			
			if(! region.isLeafRegion()) {
				throw new IllegalArgumentException("Region is not a leaf region, unable to split:" + region);
			}
		} catch (ZookeeperException e) {
			logger.error("Got exception while init region splitter", e);
			region = null;
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
		final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();
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
			final ZookeeperClient zookeperClient = ZookeeperClientFactory.getZookeeperClient();
			zookeperClient.setStateForDistributionGroup(region, DistributionRegion.STATE_SPLITTED);
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
			final List<SSTableFacade> facades = ssTableManager.getSstableFacades();
			spreadFacades(region, ssTableName, facades);
			
			// Spread in memory data
			ssTableManager.flushMemtable();
			final List<Memtable> unflushedMemtables = ssTableManager.getUnflushedMemtables();
			spreadUnflushedMemtables(region, ssTableName, unflushedMemtables);
			
		} catch (StorageManagerException e) {
			logger.warn("Got an exception while distributing tuples for: " + ssTableName, e);
		}
		
		logger.info("Redistributing table " + ssTableName.getFullname() + " is DONE");
	}
	
	/**
	 * Spread the unflushed memtables
	 * @param region 
	 * @param ssTableName 
	 * @param unflushedMemtables
	 */
	protected void spreadUnflushedMemtables(DistributionRegion region, SSTableName ssTableName, final List<Memtable> unflushedMemtables) {
		for(final Memtable memtable : unflushedMemtables) {
			logger.info("Spread metable for sstable: " + ssTableName + " with a size of " + memtable.getSizeInMemory());
			
			spreadTuplesFromIterator(region, ssTableName, memtable.iterator());
		}
	}

	/**
	 * Spread the given sstable facades
	 * @param region
	 * @param ssTableName
	 * @param facades
	 */
	protected void spreadFacades(final DistributionRegion region, final SSTableName ssTableName, final List<SSTableFacade> facades) {

		for(final SSTableFacade facade: facades) {
			final boolean acquired = facade.acquire();
			
			if(acquired) {	
				logger.info("Spread sstable facade: {}", facade.getName().getFullname());
				
				final SSTableKeyIndexReader reader = facade.getSsTableKeyIndexReader();
				
				final boolean distributeSuccessfully 
					= spreadTuplesFromIterator(region, ssTableName, reader.iterator());
				
				// Data is spread, so we can delete it
				if(! distributeSuccessfully) {
					logger.warn("Distribution of {} was not successfully", facade.getName().getFullname());
				} else {
					logger.info("Distribution of {} was successfully, scheduling for deletion", facade.getName().getFullname());
					facade.deleteOnClose();
				}
				
				facade.release();
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
				final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(instance);
				
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
		logger.info("Set split at:" + splitPosition);
		region.setSplit(splitPosition);
	
		try {
			// Allocate systems 
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			DistributionRegionHelper.allocateSystemsToNewRegion(region.getLeftChild(), zookeeperClient);
			DistributionRegionHelper.allocateSystemsToNewRegion(region.getRightChild(), zookeeperClient);
		
			// Let the data settle down
			Thread.sleep(5000);
			
		} catch (ZookeeperException e) {
			logger.warn("Unable to assign systems to splitted region: " + region, e);
		} catch (ResourceAllocationException e) {
			logger.warn("Unable to find systems for splitted region: " + region, e);
		} catch (InterruptedException e) {
			logger.warn("Got InterruptedException while wait for settle down: " + region, e);
			Thread.currentThread().interrupt();
		}
	}

}

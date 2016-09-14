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
import de.fernunihagen.dna.scalephant.storage.StorageInterface;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableKeyIndexReader;

public abstract class RegionSplitStrategy {
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RegionSplitStrategy.class);

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
	public boolean performSplit(final SSTableName ssTableName) {
		logger.info("Performing split for: " + ssTableName);
		
		if(! ssTableName.isNameprefixValid()) {
			logger.warn("Unable to split table, nameprefix is invalid: " + ssTableName);
			return false;
		}
		
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			
			final DistributionRegion distributionGroup = DistributionGroupCache.getGroupForGroupName(ssTableName.getDistributionGroup(), zookeeperClient);
			
			final DistributionRegion region = DistributionRegionHelper.getDistributionRegionForNamePrefix(distributionGroup, ssTableName.getNameprefix());
		
			if(region == null) {
				logger.warn("Unable to find nameprefix in distributionGroup: " + ssTableName);
				return false;
			}
			
			if(! region.isLeafRegion()) {
				logger.error("Region is not a leaf region, unable to split:" + region);
				return false;
			}
			
			performSplit(region);
			redistributeData(region);
		} catch (ZookeeperException e) {
			logger.warn("Unable to split sstable: " + ssTableName, e);
			return false;
		}
		
		return true;
	}

	/**
	 * Redistribute data after region split
	 * @param region
	 */
	protected void redistributeData(final DistributionRegion region) {
		logger.info("Redistributing data for region: " + region);
		
		final List<SSTableName> localTables = StorageInterface.getAllTables();
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
			final SSTableManager ssTableManager = StorageInterface.getSSTableManager(ssTableName);
			
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
				
				logger.info("Spread sstable facade: " + facade.getName());
				
				final SSTableKeyIndexReader reader = facade.getSsTableKeyIndexReader();
				spreadTuplesFromIterator(region, ssTableName, reader.iterator());
				
				facade.release();
			}
		}
	}

	/**
	 * Spread the tuples from the given iterator
	 * @param region
	 * @param ssTableName
	 * @param tupleIterator
	 */
	public void spreadTuplesFromIterator(final DistributionRegion region, final SSTableName ssTableName, final Iterator<Tuple> tupleIterator) {
		
		final DistributionRegion leftRegion = region.getLeftChild();
		final DistributionRegion rightRegion = region.getRightChild();
		
		while(tupleIterator.hasNext()) {
			
			final Tuple tuple = tupleIterator.next();
			
			// Spread to the left region
			if(tuple.getBoundingBox().overlaps(leftRegion.getConveringBox())) {
				final Collection<DistributedInstance> instances = leftRegion.getSystems();
				spreadTupleToSystems(ssTableName, tuple, instances);
			}
			
			// Spread to the right region
			if(tuple.getBoundingBox().overlaps(rightRegion.getConveringBox())) {
				final Collection<DistributedInstance> instances = rightRegion.getSystems();
				spreadTupleToSystems(ssTableName, tuple, instances);							
			}
		}
	}
	
	/**
	 * Spread the tuple to the given list of systems
	 * @param ssTableName
	 * @param tuple
	 * @param instances
	 * @return 
	 */
	protected boolean spreadTupleToSystems(final SSTableName ssTableName, final Tuple tuple, final Collection<DistributedInstance> instances) {
		final MembershipConnectionService membershipConnectionService = MembershipConnectionService.getInstance();
		
		for(final DistributedInstance instance : instances) {
			final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(instance);
			
			if(connection == null) {
				logger.warn("Connection to system is not known, unable to distribute tuple: " + connection);
				return false;
			}
			
			connection.insertTuple(ssTableName.getFullname(), tuple);
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
		}
	}
}

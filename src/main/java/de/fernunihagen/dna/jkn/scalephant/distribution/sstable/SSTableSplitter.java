package de.fernunihagen.dna.jkn.scalephant.distribution.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClientFactory;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

public abstract class SSTableSplitter {
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(SSTableSplitter.class);

	/**
	 * Is a split needed?
	 * @param totalTuplesInTable
	 * @return
	 */
	public abstract boolean isSplitNeeded(final int totalTuplesInTable);
	
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
			
			final DistributionRegion distributionGroup = zookeeperClient.readDistributionGroup(
					ssTableName.getDistributionGroup());
			
			final DistributionRegion region = distributionGroup.getDistributionRegionForNamePrefix(ssTableName.getNameprefix());
			
			if(region == null) {
				logger.warn("Unable to find nameprefix in distributionGroup: " + ssTableName);
				return false;
			}
			
			if(! region.isLeafRegion()) {
				logger.error("Region is not a leaf region, unable to split:" + region);
				return false;
			}
			
			performSplit(region);
			
		} catch (ZookeeperException e) {
			logger.warn("Unable to split sstable: " + ssTableName, e);
			return false;
		}
		
		return true;
	}
	
}

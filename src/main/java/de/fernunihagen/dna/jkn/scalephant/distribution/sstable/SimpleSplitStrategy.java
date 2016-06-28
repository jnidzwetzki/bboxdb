package de.fernunihagen.dna.jkn.scalephant.distribution.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

public class SimpleSplitStrategy implements SSTableSplitter  {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableSplitter.class);


	/**
	 * Test if a split is needed
	 */
	@Override
	public boolean isSplitNeeded(final int totalTuplesInTable) {
		final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();
		final int maxEntries = configuration.getSstableMaxEntries();
		
		if(totalTuplesInTable > maxEntries) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Perform a split of the given sstable
	 */
	@Override
	public void performSplit(final SSTableName ssTableName) {
		logger.info("Performing split for: " + ssTableName);
	}


}

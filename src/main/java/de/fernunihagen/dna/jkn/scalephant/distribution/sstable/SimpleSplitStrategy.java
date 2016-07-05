package de.fernunihagen.dna.jkn.scalephant.distribution.sstable;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.resource.RandomResourcePlacementStrategy;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.FloatInterval;

public class SimpleSplitStrategy extends SSTableSplitter {
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(SimpleSplitStrategy.class);

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
	protected void performSplit(final DistributionRegion region) {
		
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		final Set<DistributedInstance> systems = distributedInstanceManager.getInstances();
		
		if(systems.isEmpty()) {
			logger.warn("Unable to split region, no ressources are avilable: " + region);
			return;
		}
		
		logger.info("Performing split of region: " + region);
		
		// Split region
		final int splitDimension = region.getSplitDimension();
		final FloatInterval interval = region.getConveringBox().getIntervalForDimension(splitDimension);
		
		logger.info("Split at dimension:" + splitDimension + " interval: " + interval);
		float midpoint = interval.getMidpoint();
		
		logger.info("Set split at:" + midpoint);
		region.setSplit(midpoint);
		
		// Assign systems
		final RandomResourcePlacementStrategy resourcePlacementStrategy = new RandomResourcePlacementStrategy();
		
		final DistributedInstance systemLeftChild = resourcePlacementStrategy.findSystemToAllocate(systems);
		final DistributedInstance systemRightChild = resourcePlacementStrategy.findSystemToAllocate(systems);
		
		region.getLeftChild().addSystem(systemLeftChild);
		region.getRightChild().addSystem(systemRightChild);
	}
}

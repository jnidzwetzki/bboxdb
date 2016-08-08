package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.placement.ResourceAllocationException;
import de.fernunihagen.dna.jkn.scalephant.distribution.placement.ResourcePlacementStrategy;
import de.fernunihagen.dna.jkn.scalephant.distribution.placement.ResourcePlacementStrategyFactory;

public class DistributionRegionHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionHelper.class);

	
	public static void allocateSystemsToNewRegion(final DistributionRegion region, final ZookeeperClient zookeeperClient) throws ZookeeperException, ResourceAllocationException {
		
		final short replicationFactor = zookeeperClient.getReplicationFactorForDistributionGroup(region.getName());
		
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		final List<DistributedInstance> availableSystems = distributedInstanceManager.getInstances();
		
		final ResourcePlacementStrategy resourcePlacementStrategy = ResourcePlacementStrategyFactory.getInstance();

		// The blacklist, to prevent duplicate allocations
		final Set<DistributedInstance> allocationSystems = new HashSet<DistributedInstance>();
		
		for(short i = 0; i < replicationFactor; i++) {
			final DistributedInstance instance = resourcePlacementStrategy.getInstancesForNewRessource(availableSystems, allocationSystems);
			allocationSystems.add(instance);
		}
		
		logger.info("Allocating region " + region.getName() + " to " + allocationSystems);
		
		// Resource allocation successfully, write data to zookeeper
		for(final DistributedInstance instance : allocationSystems) {
			zookeeperClient.addSystemToDistributionRegion(region, instance);
		}
	}
}

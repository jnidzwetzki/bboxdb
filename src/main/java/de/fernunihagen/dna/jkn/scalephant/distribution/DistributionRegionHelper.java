package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.placement.ResourceAllocationException;
import de.fernunihagen.dna.jkn.scalephant.distribution.placement.ResourcePlacementStrategy;
import de.fernunihagen.dna.jkn.scalephant.distribution.placement.ResourcePlacementStrategyFactory;
import de.fernunihagen.dna.jkn.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.zookeeper.ZookeeperException;

public class DistributionRegionHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionHelper.class);

	/**
	 * Allocate the required amount of systems to the given region
	 * 
	 * @param region
	 * @param zookeeperClient
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 */
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
	
	/**
	 * Find the region for the given name prefix
	 * @param searchNameprefix
	 * @return
	 */
	public static DistributionRegion getDistributionRegionForNamePrefix(final DistributionRegion region, final int searchNameprefix) {
		final DistributionRegionNameprefixFinder distributionRegionNameprefixFinder = new DistributionRegionNameprefixFinder(searchNameprefix);
		region.visit(distributionRegionNameprefixFinder);
		
		return distributionRegionNameprefixFinder.getResult();
	}
	
	/**
	 * Calculate the amount of regions each DistributedInstance is responsible
	 * @param region
	 * @return
	 */
	public static Map<DistributedInstance, Integer> getSystemUtilization(final DistributionRegion region) {
		final CalculateSystemUtilization calculateSystemUtilization = new CalculateSystemUtilization();
		region.visit(calculateSystemUtilization);
		
		return calculateSystemUtilization.getResult();
	}
	
}

class DistributionRegionNameprefixFinder implements DistributionRegionVisitor {

	/**
	 * The name prefix to search
	 */
	protected int nameprefix;
	
	/**
	 * The result
	 */
	protected DistributionRegion result = null;
	
	public DistributionRegionNameprefixFinder(final int nameprefix) {
		this.nameprefix = nameprefix;
	}
	
	@Override
	public boolean visitRegion(final DistributionRegion distributionRegion) {
		
		if(distributionRegion.getNameprefix() == nameprefix) {
			result = distributionRegion;
		}
		
		// Stop search
		if(result != null) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Get the result
	 * @return
	 */
	public DistributionRegion getResult() {
		return result;
	}
}

class CalculateSystemUtilization implements DistributionRegionVisitor {

	/**
	 * The utilization
	 */
	protected Map<DistributedInstance, Integer> utilization = new HashMap<DistributedInstance, Integer>();
	
	@Override
	public boolean visitRegion(final DistributionRegion distributionRegion) {
	
		final Collection<DistributedInstance> systems = distributionRegion.getSystems();
		
		for(final DistributedInstance instance : systems) {
			if(! utilization.containsKey(instance)) {
				utilization.put(instance, 1);
			} else {
				int oldValue = utilization.get(instance);
				oldValue++;
				utilization.put(instance, oldValue);
			}
		}
		
		// Visit remaining nodes
		return true;
	}
	
	/**
	 * Get the result
	 * @return
	 */
	public Map<DistributedInstance, Integer> getResult() {
		return utilization;
	}
	
}

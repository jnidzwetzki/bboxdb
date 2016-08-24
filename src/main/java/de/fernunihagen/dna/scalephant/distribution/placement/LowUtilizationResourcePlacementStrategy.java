package de.fernunihagen.dna.scalephant.distribution.placement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupName;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegionHelper;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;

public class LowUtilizationResourcePlacementStrategy extends ResourcePlacementStrategy {
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(LowUtilizationResourcePlacementStrategy.class);
	
	public LowUtilizationResourcePlacementStrategy() {

	}
	
	@Override
	public DistributedInstance getInstancesForNewRessource(final List<DistributedInstance> systems, final Collection<DistributedInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		final List<DistributedInstance> availableSystems = new ArrayList<DistributedInstance>(systems);
		availableSystems.removeAll(blacklist);
		
		if(availableSystems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted");
		}
		
		try {
			final Map<DistributedInstance, Integer> systemUsage = calculateSystemUsage();
			return getSystemWithLowestUsage(availableSystems, systemUsage);
		} catch (ZookeeperException e) {
			throw new ResourceAllocationException("Got an zookeeper exception while ressource allocation", e);
		}		
	}

	/**
	 * Find and return the system with the lowest usage
	 * 
	 * @param availableSystems
	 * @param systemUsage
	 * @return
	 */
	protected DistributedInstance getSystemWithLowestUsage(final List<DistributedInstance> availableSystems, final Map<DistributedInstance, Integer> systemUsage) {
		
		DistributedInstance possibleSystem = null;
		
		for(final DistributedInstance distributedInstance : availableSystems) {
			
			// Unknown = Empty instance
			if(! systemUsage.containsKey(distributedInstance)) {
				return distributedInstance;
			}
			
			if(possibleSystem == null) {
				possibleSystem = distributedInstance;
			} else {
				if(systemUsage.get(possibleSystem) > systemUsage.get(distributedInstance)) {
					possibleSystem = distributedInstance;
				}
			}
		}
		
		return possibleSystem;
	}

	/**
	 * Calculate the usage of each system
	 * 
	 * @return
	 * @throws ZookeeperException
	 */
	protected Map<DistributedInstance, Integer> calculateSystemUsage() throws ZookeeperException {
		
		// The overall usage
		final Map<DistributedInstance, Integer> systemUsage = new HashMap<DistributedInstance, Integer>();
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		final List<DistributionGroupName> distributionGroups = zookeeperClient.getDistributionGroups();
		
		// Calculate usage for each distribution group
		for(final DistributionGroupName groupName : distributionGroups) {
			final DistributionRegion region = DistributionGroupCache.getGroupForGroupName(groupName.getFullname(), zookeeperClient);
			final Map<DistributedInstance, Integer> regionSystemUsage = DistributionRegionHelper.getSystemUtilization(region);
		
			// Merge result into systemUsage
			for(final DistributedInstance instance : regionSystemUsage.keySet()) {
				if(! systemUsage.containsKey(instance)) {
					systemUsage.put(instance, regionSystemUsage.get(instance));
				} else {
					int oldUsage = systemUsage.get(instance);
					int newUsage = oldUsage + regionSystemUsage.get(instance);
					systemUsage.put(instance, newUsage);
				}
			}
		}
		
		return systemUsage;
	}
}

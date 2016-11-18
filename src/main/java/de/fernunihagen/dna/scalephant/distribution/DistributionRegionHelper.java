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
package de.fernunihagen.dna.scalephant.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.scalephant.distribution.placement.ResourceAllocationException;
import de.fernunihagen.dna.scalephant.distribution.placement.ResourcePlacementStrategy;
import de.fernunihagen.dna.scalephant.distribution.placement.ResourcePlacementStrategyFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;

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
	
	/**
	 * Find the outdated regions for the distributed instance
	 * @param region
	 * @param distributedInstance
	 * @return
	 */
	public static List<OutdatedDistributionRegion> getOutdatedRegions(final DistributionRegion region, final DistributedInstance distributedInstance) {
		final DistributionRegionOutdatedRegionFinder distributionRegionOutdatedRegionFinder = new DistributionRegionOutdatedRegionFinder(distributedInstance);
		region.visit(distributionRegionOutdatedRegionFinder);
		
		return distributionRegionOutdatedRegionFinder.getResult();
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

class DistributionRegionOutdatedRegionFinder implements DistributionRegionVisitor {
	
	/**
	 * The instance
	 */
	protected final DistributedInstance instanceToSearch;
	
	/**
	 * The result of the operation
	 */
	protected final List<OutdatedDistributionRegion> result;
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionOutdatedRegionFinder.class);

	public DistributionRegionOutdatedRegionFinder(final DistributedInstance instanceToSearch) {
		this.instanceToSearch = instanceToSearch;
		this.result = new ArrayList<OutdatedDistributionRegion>();
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
	}
 	
	@Override
	public boolean visitRegion(final DistributionRegion distributionRegion) {
		
		if(! isInstanceContained(distributionRegion)) {
			return true;
		}
		
		try {
			DistributedInstance newestInstance = null;
			long newestVersion = Long.MIN_VALUE;
			
			for(final DistributedInstance instance : distributionRegion.getSystems()) {
				if(! instance.socketAddressEquals(instance)) {
					
					long version = zookeeperClient.getCheckpointForDistributionRegion(distributionRegion, instance);
					
					if(newestVersion < version) {
						newestInstance = instance;
						newestVersion = version;
					} 
				}
			}
			
			final long localVersion = zookeeperClient.getCheckpointForDistributionRegion(distributionRegion, instanceToSearch);

			if(newestVersion > localVersion) {
				result.add(new OutdatedDistributionRegion(distributionRegion, newestInstance, localVersion));
			}
		} catch (ZookeeperException e) {
			logger.error("Got exception while check for outdated regions", e);
		}
		
		// Visit remaining nodes
		return true;
	}

	/**
	 * Is the local instance contained in the distribution region
	 * @param distributionRegion
	 * @return
	 */
	protected boolean isInstanceContained(final DistributionRegion distributionRegion) {

		for(final DistributedInstance instance : distributionRegion.getSystems()) {
			if(instance.socketAddressEquals(instanceToSearch)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Get the result of the operation
	 * @return
	 */
	public List<OutdatedDistributionRegion> getResult() {
		return result;
	}
	
}

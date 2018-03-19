/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.distribution.partitioner;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.placement.ResourcePlacementStrategy;
import org.bboxdb.distribution.placement.ResourcePlacementStrategyFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpacePartitionerHelper {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SpacePartitionerHelper.class);
	
	/**
	 * Copy the system allocation of one distribution region to another
	 * @param source
	 * @param destination
	 * @throws ZookeeperException 
	 */
	public static void copySystemsToRegion(final List<BBoxDBInstance> systems, 
			final String destinationPath, final ZookeeperClient client) 
					throws ZookeeperException {
		
		assert (! systems.isEmpty()) : "Systems are empty: " + systems;
		
		for(final BBoxDBInstance system : systems) {
			client.getDistributionRegionAdapter().addSystemToDistributionRegion(destinationPath, system);
		}
	}
	
	/**
	 * Allocate the required amount of systems to the given region
	 * 
	 * @param region
	 * @param zookeeperClient
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 * @throws ZookeeperNotFoundException 
	 */
	public static void allocateSystemsToRegion(final String regionPath,
			final String distributionGroupName,
			final Collection<BBoxDBInstance> blacklist, 
			final ZookeeperClient zookeeperClient) 
					throws ZookeeperException, ResourceAllocationException, ZookeeperNotFoundException {
				
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(distributionGroupName);

		final short replicationFactor = config.getReplicationFactor();
		
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		final List<BBoxDBInstance> availableSystems = distributedInstanceManager.getInstances();
		
		final String placementStrategy = config.getPlacementStrategy();
		
		final ResourcePlacementStrategy resourcePlacementStrategy 
			= ResourcePlacementStrategyFactory.getInstance(placementStrategy);

		if(resourcePlacementStrategy == null) {
			throw new ResourceAllocationException("Unable to instanciate the ressource "
					+ "placement strategy");
		}
		
		// The blacklist, to prevent duplicate allocations
		final Set<BBoxDBInstance> allocationSystems = new HashSet<>();
		final Set<BBoxDBInstance> blacklistedSystems = new HashSet<>();
		blacklistedSystems.addAll(blacklist);
				
		for(short i = 0; i < replicationFactor; i++) {
			final BBoxDBInstance instance = resourcePlacementStrategy.getInstancesForNewRessource(availableSystems, blacklistedSystems);
			allocationSystems.add(instance);
			blacklistedSystems.add(instance);
		}
		
		logger.info("Allocated new ressource to {} with blacklist {}", 
				allocationSystems, blacklist);

		zookeeperClient.getDistributionRegionAdapter()
			.allocateSystemsToRegion(regionPath, allocationSystems);
	}
}

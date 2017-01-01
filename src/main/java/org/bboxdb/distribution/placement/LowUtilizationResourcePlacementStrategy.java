/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.distribution.placement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		PlacementHelper.removeAllNonReadySystems(availableSystems);
		
		if(availableSystems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted");
		}
		
		try {
			final Map<DistributedInstance, Integer> systemUsage = calculateSystemUsage();
			return getSystemWithLowestUsage(availableSystems, systemUsage);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
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
	 * @throws ZookeeperNotFoundException 
	 */
	protected Map<DistributedInstance, Integer> calculateSystemUsage() throws ZookeeperException, ZookeeperNotFoundException {
		
		// The overall usage
		final Map<DistributedInstance, Integer> systemUsage = new HashMap<DistributedInstance, Integer>();
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
		final DistributionGroupZookeeperAdapter zookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
		final List<DistributionGroupName> distributionGroups = zookeeperAdapter.getDistributionGroups(null);
		
		// Calculate usage for each distribution group
		for(final DistributionGroupName groupName : distributionGroups) {
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForGroupName(groupName.getFullname(), zookeeperClient);
			final DistributionRegion region = distributionAdapter.getRootNode();
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

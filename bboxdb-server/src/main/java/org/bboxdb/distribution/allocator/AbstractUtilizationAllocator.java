/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.distribution.allocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;

public abstract class AbstractUtilizationAllocator extends AbstractResourceAllocator {
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(AbstractUtilizationAllocator.class);
	
	public AbstractUtilizationAllocator() {

	}
	
	/**
	 * Get the system with the lowest cpu core / instance relation
	 * @param availableSystems
	 * @param systemUsage
	 * @return
	 * @throws ResourceAllocationException
	 */
	protected BBoxDBInstance getSystemWithLowestUsage(final List<BBoxDBInstance> availableSystems, 
			final Multiset<BBoxDBInstance> systemUsage) throws ResourceAllocationException {
		
		// Unknown = Empty instance
		final BBoxDBInstance emptyInstance = availableSystems.stream()
			.filter(i -> systemUsage.count(i) == 0)
			.findAny()
			.orElse(null);
		
		if(emptyInstance != null) {
			return emptyInstance;
		}
		
		return availableSystems.stream()
			.filter(getUnusableSystemsFilterPredicate())
			.reduce((a, b) -> calculateUsageFactor(systemUsage, a) > calculateUsageFactor(systemUsage, b) ? 
					a : b)
			.orElse(null);
	}

	/**
	 * Calculate the usage of each system
	 * 
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	protected Multiset<BBoxDBInstance> calculateSystemUsage() 
			throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
				
		final DistributionGroupAdapter zookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		
		final List<String> distributionGroups = zookeeperAdapter.getDistributionGroups();
		
		// The overall usage
	    final ImmutableMultiset.Builder<BBoxDBInstance> builder = ImmutableMultiset.builder();
	    
		// Calculate usage for each distribution group
		for(final String groupName : distributionGroups) {
			final SpacePartitioner spacepartitioner = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(groupName);
			
			final DistributionRegion region = spacepartitioner.getRootNode();
			final Multiset<BBoxDBInstance> regionSystemUsage 
				= DistributionRegionHelper.getSystemUtilization(region);
		
			// Merge results
			builder.addAll(regionSystemUsage);
		}
		
	    return builder.build();
	}
	

	@Override
	public BBoxDBInstance getInstancesForNewRessource(final List<BBoxDBInstance> systems, 
			final Collection<BBoxDBInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		final List<BBoxDBInstance> availableSystems = new ArrayList<>(systems);
		availableSystems.removeAll(blacklist);
		removeAllNonReadySystems(availableSystems);
		
		if(availableSystems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted");
		}
		
		try {
			final Multiset<BBoxDBInstance> systemUsage = calculateSystemUsage();
			return getSystemWithLowestUsage(availableSystems, systemUsage);
		} catch (Exception e) {
			throw new ResourceAllocationException("Got an zookeeper exception while ressource allocation", e);
		}		
	}
	
	/**
	 * Filter unusable instances
	 * @return
	 */
	protected abstract Predicate<? super BBoxDBInstance> getUnusableSystemsFilterPredicate();

	/**
	 * Calculate the usage factor
	 * @param systemUsage
	 * @param distributedInstance
	 * @return
	 */
	protected abstract double calculateUsageFactor(final Multiset<BBoxDBInstance> systemUsage,
			final BBoxDBInstance distributedInstance);

}

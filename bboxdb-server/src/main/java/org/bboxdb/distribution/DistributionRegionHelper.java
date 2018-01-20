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
package org.bboxdb.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.bboxdb.commons.Retryer;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class DistributionRegionHelper {
	
	/**
	 * The states for read operations
	 */
	private final static Collection<DistributionRegionState> STATES_READ = Arrays.asList(
			DistributionRegionState.ACTIVE, 
			DistributionRegionState.ACTIVE_FULL, 
			DistributionRegionState.SPLITTING, 
			DistributionRegionState.MERGING);
	/**
	 * System for read operations
	 */
	public static Predicate<DistributionRegionState> PREDICATE_REGIONS_FOR_READ 
		= (s) -> (STATES_READ.contains(s));
	
	/**
	 * The states for write operations
	 */
	private	final static Collection<DistributionRegionState> STATES_WRITE = Arrays.asList(
				DistributionRegionState.ACTIVE, 
				DistributionRegionState.ACTIVE_FULL);
	/**
	 * Systems for write operations
	 */
	public static Predicate<DistributionRegionState> PREDICATE_REGIONS_FOR_WRITE 
		= (s) -> (STATES_WRITE.contains(s));
	
	/**
	 * Find the region for the given name prefix
	 * @param searchNameprefix
	 * @return
	 * @throws InterruptedException 
	 */
	public static DistributionRegion getDistributionRegionForNamePrefix(
			final DistributionRegion region, final long searchNameprefix) throws InterruptedException {
		
		if(region == null) {
			return null;
		}
		
		final Callable<DistributionRegion> getDistributionRegion = new Callable<DistributionRegion>() {

			@Override
			public DistributionRegion call() throws Exception {
				final DistributionRegionNameprefixFinder distributionRegionNameprefixFinder 
					= new DistributionRegionNameprefixFinder(searchNameprefix);

				region.visit(distributionRegionNameprefixFinder);
				
				final DistributionRegion result = distributionRegionNameprefixFinder.getResult();
			
				if(result == null) {
					throw new Exception("Unable to get distribution region");
				}
				
				return result;
			}
		};
		
		// Retry the operation if neeed
		final Retryer<DistributionRegion> retyer = new Retryer<>(Const.OPERATION_RETRY, 
				250, 
				getDistributionRegion);
		
		retyer.execute();
		
		return retyer.getResult();
	}
	
	/**
	 * Calculate the amount of regions each DistributedInstance is responsible
	 * @param region
	 * @return
	 */
	public static Multiset<BBoxDBInstance> getSystemUtilization(final DistributionRegion region) {
		final CalculateSystemUtilization calculateSystemUtilization = new CalculateSystemUtilization();
		
		if(region != null) {
			region.visit(calculateSystemUtilization);
		}
		
		return calculateSystemUtilization.getResult();
	}
	
	/**
	 * Find the outdated regions for the distributed instance
	 * @param region
	 * @param distributedInstance
	 * @return
	 */
	public static List<OutdatedDistributionRegion> getOutdatedRegions(final DistributionRegion region, 
			final BBoxDBInstance distributedInstance) {
		
		final DistributionRegionOutdatedRegionFinder distributionRegionOutdatedRegionFinder 
			= new DistributionRegionOutdatedRegionFinder(distributedInstance);
		
		if(region != null) {
			region.visit(distributionRegionOutdatedRegionFinder);
		}
		
		return distributionRegionOutdatedRegionFinder.getResult();
	}
	
}

class DistributionRegionNameprefixFinder implements DistributionRegionVisitor {

	/**
	 * The name prefix to search
	 */
	protected long nameprefix;
	
	/**
	 * The result
	 */
	protected DistributionRegion result = null;
	
	public DistributionRegionNameprefixFinder(final long nameprefix) {
		this.nameprefix = nameprefix;
	}
	
	@Override
	public boolean visitRegion(final DistributionRegion distributionRegion) {
		
		if(distributionRegion.getRegionId() == nameprefix) {
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
	protected Multiset<BBoxDBInstance> utilization = HashMultiset.create();
	
	@Override
	public boolean visitRegion(final DistributionRegion distributionRegion) {
		final Collection<BBoxDBInstance> systems = distributionRegion.getSystems();
		systems.forEach(i -> utilization.add(i));

		// Visit remaining nodes
		return true;
	}
	
	/**
	 * Get the result
	 * @return
	 */
	public Multiset<BBoxDBInstance> getResult() {
		return utilization;
	}
	
}

class DistributionRegionOutdatedRegionFinder implements DistributionRegionVisitor {
	
	/**
	 * The instance
	 */
	protected final BBoxDBInstance instanceToSearch;
	
	/**
	 * The result of the operation
	 */
	protected final List<OutdatedDistributionRegion> result;

	/**
	 * The zookeeper adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionOutdatedRegionFinder.class);

	public DistributionRegionOutdatedRegionFinder(final BBoxDBInstance instanceToSearch) {
		this.instanceToSearch = instanceToSearch;
		this.result = new ArrayList<OutdatedDistributionRegion>();
		this.distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	}
 	
	@Override
	public boolean visitRegion(final DistributionRegion distributionRegion) {
		
		if(! isInstanceContained(distributionRegion)) {
			return true;
		}
		
		try {
			BBoxDBInstance newestInstance = null;
			long newestVersion = Long.MIN_VALUE;
			
			for(final BBoxDBInstance instance : distributionRegion.getSystems()) {
				if(instance.socketAddressEquals(instanceToSearch)) {
					continue;
				}
					
				final long version 
					= distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(
							distributionRegion, instance);
				
				if(newestVersion < version) {
					newestInstance = instance;
					newestVersion = version;
				} 
				
			}
			
			final long localVersion = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(distributionRegion, instanceToSearch);

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
		return distributionRegion.getSystems()
			.stream()
			.anyMatch(s -> s.socketAddressEquals(instanceToSearch));
	}
	
	/**
	 * Get the result of the operation
	 * @return
	 */
	public List<OutdatedDistributionRegion> getResult() {
		return result;
	}
}

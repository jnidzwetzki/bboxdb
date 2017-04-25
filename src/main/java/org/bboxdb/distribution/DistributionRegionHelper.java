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
package org.bboxdb.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionHelper {
	
	/**
	 * Find the region for the given name prefix
	 * @param searchNameprefix
	 * @return
	 */
	public static DistributionRegion getDistributionRegionForNamePrefix(final DistributionRegion region, final int searchNameprefix) {
		
		if(region == null) {
			return null;
		}
		
		for(int retry = 0; retry < Const.OPERATION_RETRY; retry++) {
			final DistributionRegionNameprefixFinder distributionRegionNameprefixFinder 
				= new DistributionRegionNameprefixFinder(searchNameprefix);

			region.visit(distributionRegionNameprefixFinder);
				
			final DistributionRegion result = distributionRegionNameprefixFinder.getResult();
			
			if(result != null) {
				return result;
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return null;
			}
		}
		
		return null;
	}
	
	/**
	 * Calculate the amount of regions each DistributedInstance is responsible
	 * @param region
	 * @return
	 */
	public static Map<DistributedInstance, Integer> getSystemUtilization(final DistributionRegion region) {
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
			final DistributedInstance distributedInstance) {
		
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
	 * The zookeeper adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionOutdatedRegionFinder.class);

	public DistributionRegionOutdatedRegionFinder(final DistributedInstance instanceToSearch) {
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
			DistributedInstance newestInstance = null;
			long newestVersion = Long.MIN_VALUE;
			
			for(final DistributedInstance instance : distributionRegion.getSystems()) {
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

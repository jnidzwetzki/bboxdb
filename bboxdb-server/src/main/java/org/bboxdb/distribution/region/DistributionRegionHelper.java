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
package org.bboxdb.distribution.region;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bboxdb.commons.Retryer;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.OutdatedDistributionRegion;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.network.routing.RoutingHop;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class DistributionRegionHelper {
	
	/**
	 * The states for read operations
	 */
	public final static Collection<DistributionRegionState> STATES_READ = Arrays.asList(
			DistributionRegionState.ACTIVE, 
			DistributionRegionState.ACTIVE_FULL, 
			DistributionRegionState.SPLITTING, 
			DistributionRegionState.REDISTRIBUTION_ACTIVE,
			DistributionRegionState.MERGING);
	
	/**
	 * System for read operations
	 */
	public static Predicate<DistributionRegionState> PREDICATE_REGIONS_FOR_READ 
		= (s) -> (STATES_READ.contains(s));
	 
	/**
	 * The states for write operations
	 */
	public final static Collection<DistributionRegionState> STATES_WRITE = Arrays.asList(
				DistributionRegionState.ACTIVE, 
				DistributionRegionState.ACTIVE_FULL,
				DistributionRegionState.REDISTRIBUTION_ACTIVE);
	/**
	 * Systems for write operations
	 */
	public static Predicate<DistributionRegionState> PREDICATE_REGIONS_FOR_WRITE 
		= (s) -> (STATES_WRITE.contains(s));
	
	/**
	 * Add the leaf nodes systems that are covered by the bounding box
	 * @param rootRegion 
	 * @param boundingBox
	 * @param systems
	 * @return 
	 */
	public static List<RoutingHop> getRegionsForPredicateAndBox(
			final DistributionRegion rootRegion, final Hyperrectangle boundingBox, 
			final Predicate<DistributionRegionState> statePredicate) {
	
		final Map<InetSocketAddress, RoutingHop> hops = new HashMap<>();
		
		final List<DistributionRegion> regions = rootRegion.getThisAndChildRegions();
		
		for(final DistributionRegion region : regions) {
			
			if(! boundingBox.intersects(region.getConveringBox())) {
				continue;
			}
			
			if(! statePredicate.test(region.getState())) {
				continue;
			}
			
			for(final BBoxDBInstance system : region.getSystems()) {
				if(! hops.containsKey(system.getInetSocketAddress())) {
					final RoutingHop routingHop = new RoutingHop(system, new ArrayList<Long>());
					hops.put(system.getInetSocketAddress(), routingHop);
				}
				
				final RoutingHop routingHop = hops.get(system.getInetSocketAddress());
				routingHop.addRegion(region.getRegionId());
			}
		}
		
		return new ArrayList<>(hops.values());
	}
		
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
				return region
					.getThisAndChildRegions()
					.stream()
					.filter(r -> r.getRegionId() == searchNameprefix)
					.findAny().orElseThrow(() -> new Exception("Unable to get distribution region"));
			}
		};
		
		// Retry the operation if needed
		final Retryer<DistributionRegion> retyer = new Retryer<>(Const.OPERATION_RETRY, 
				250, 
				TimeUnit.MILLISECONDS,
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
		
		final Multiset<BBoxDBInstance> utilization = HashMultiset.create();
		
		if(region != null) {
			region
				.getThisAndChildRegions()
				.stream()
				.map(r -> r.getSystems())
				.flatMap(s -> s.stream())
				.forEach(s -> utilization.add(s));
		}
		
		return utilization;
	}
	
	/**
	 * Find the outdated regions for the distributed instance
	 * @param region
	 * @param distributedInstance
	 * @return
	 * @throws BBoxDBException 
	 */
	public static List<OutdatedDistributionRegion> getOutdatedRegions(final DistributionRegion region, 
			final BBoxDBInstance distributedInstance) throws BBoxDBException {
		
		final List<OutdatedDistributionRegion> result = new ArrayList<>();
		
		if(region == null) {
			return result;
		}
		
		final List<DistributionRegion> regions = region.getThisAndChildRegions().stream()
			.filter(r -> r.getSystems().contains(distributedInstance))
			.collect(Collectors.toList());
		
		for(final DistributionRegion regionToInspect : regions) {
			try {
				final OutdatedDistributionRegion regionResult 
					= processRegion(distributedInstance, ZookeeperClientFactory.getZookeeperClient(), 
							regionToInspect);
				
				if(regionResult != null) {
					result.add(regionResult);
				}
			} catch (ZookeeperException e) {
				throw new BBoxDBException(e);
			} 
		}
		
		return result;
	}

	/**
	 * Test if there is an outdated region
	 * 
	 * @param distributedInstance
	 * @param result
	 * @param distributionGroupZookeeperAdapter
	 * @param regionToInspect
	 * @return 
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 */
	private static OutdatedDistributionRegion processRegion(final BBoxDBInstance distributedInstance,
			final ZookeeperClient zookeeperClient,
			final DistributionRegion regionToInspect) throws ZookeeperException, BBoxDBException {
		
		final Map<BBoxDBInstance, Long> versions = new HashMap<>();
		
		for(final BBoxDBInstance instance : regionToInspect.getSystems()) {
			final long version 
				= zookeeperClient.getDistributionRegionAdapter()
					.getCheckpointForDistributionRegion(regionToInspect, instance);
			
			versions.put(instance, version);
		}
		
		if(! versions.containsKey(distributedInstance)) {
			throw new BBoxDBException("Unable to find local instance for region: " 
					+ distributedInstance + " / "+ regionToInspect);
		}
		
		final Optional<Entry<BBoxDBInstance, Long>> newestInstanceOptional = versions.entrySet()
				.stream()
				.reduce((a, b) -> a.getValue() > b.getValue() ? a : b);
		
		if(! newestInstanceOptional.isPresent()) {
			return null;
		}
		
		final long localVersion = versions.get(distributedInstance);
		final Entry<BBoxDBInstance, Long> newestInstance = newestInstanceOptional.get();
		
		if(! newestInstance.getKey().equals(distributedInstance)) {
			return new OutdatedDistributionRegion(regionToInspect, newestInstance.getKey(), localVersion);
		}
		
		return null;
	}
	
	
}
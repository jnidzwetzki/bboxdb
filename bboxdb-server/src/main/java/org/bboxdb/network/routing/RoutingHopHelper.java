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
package org.bboxdb.network.routing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingHopHelper {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RoutingHopHelper.class);

	/**
	 * Get the a list of systems for the bounding box
	 * @param routingOptions 
	 * @return
	 */
	public static List<RoutingHop> getRoutingHopsForRead(final DistributionRegion rootRegion,
			final Hyperrectangle boundingBox, final EnumSet<DistributionRegionHandlingFlag> extraOptions) {

		final List<BBoxDBInstance> instances = MembershipConnectionService.getInstance().getAllInstances();

		final Map<Predicate<DistributionRegionState>, EnumSet<DistributionRegionHandlingFlag>> predicateMap = new HashMap<>();
		
		final EnumSet<DistributionRegionHandlingFlag> readOptions = EnumSet.noneOf(DistributionRegionHandlingFlag.class);
		readOptions.addAll(extraOptions);

		predicateMap.put(DistributionRegionHelper.PREDICATE_REGIONS_FOR_READ, readOptions);
		
		return getHopListForPredicateAndBox(rootRegion, boundingBox, instances, predicateMap);
	}

	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public static List<RoutingHop> getRoutingHopsForWrite(final DistributionRegion rootRegion,
			final Hyperrectangle boundingBox, final Set<DistributionRegionHandlingFlag> extraOptions) {

		final List<BBoxDBInstance> instances = MembershipConnectionService.getInstance().getAllInstances();
		
		final Map<Predicate<DistributionRegionState>, EnumSet<DistributionRegionHandlingFlag>> predicateMap = new HashMap<>();
		
		final EnumSet<DistributionRegionHandlingFlag> writeOptions = EnumSet.noneOf(DistributionRegionHandlingFlag.class);
		writeOptions.addAll(extraOptions);
		predicateMap.put(DistributionRegionHelper.PREDICATE_REGIONS_FOR_WRITE, writeOptions);
		
		// Ensure the tuple is also send to the in merging and splitting state regions to trigger 
		// the continuous queries
		final EnumSet<DistributionRegionHandlingFlag> streamOptions = EnumSet.noneOf(DistributionRegionHandlingFlag.class);
		streamOptions.addAll(extraOptions);
		streamOptions.add(DistributionRegionHandlingFlag.STREAMING_ONLY);
		predicateMap.put(DistributionRegionHelper.PREDICATE_REGIONS_FOR_STREAM, streamOptions);
		
		return getHopListForPredicateAndBox(rootRegion, boundingBox, instances, predicateMap);
	}

	/**
	 * Get a routing list for the given predicate
	 *
	 * @param rootRegion
	 * @param boundingBox
	 * @param systems
	 * @return
	 */
	public static List<RoutingHop> getHopListForPredicateAndBox(
			final DistributionRegion rootRegion, final Hyperrectangle boundingBox,
			final List<BBoxDBInstance> knownInstances,
			final Map<Predicate<DistributionRegionState>, EnumSet<DistributionRegionHandlingFlag>> statesAndOptions) {

		final Map<List<DistributionRegion>, EnumSet<DistributionRegionHandlingFlag>> routingList = new HashMap<>();
		
		for(final Map.Entry<Predicate<DistributionRegionState>, EnumSet<DistributionRegionHandlingFlag>> state : statesAndOptions.entrySet()) {
			final List<DistributionRegion> regions = getRegionsForPredicate(rootRegion, boundingBox, state.getKey());
			routingList.put(regions, state.getValue());
		}

		final Map<InetSocketAddress, RoutingHop> hops = getHopListForRegion(routingList);

		return removeUnavailableHops(knownInstances, hops);
	}

	/**
	 * Get systems for the given predicate
	 * 
	 * @param rootRegion
	 * @param boundingBox
	 * @param statePredicate
	 * @return
	 */
	public static List<DistributionRegion> getRegionsForPredicate(final DistributionRegion rootRegion,
			final Hyperrectangle boundingBox, final Predicate<DistributionRegionState> statePredicate) {
		
		final Predicate<DistributionRegion> predicate = (d) -> {
			return statePredicate.test(d.getState()) && d.getConveringBox().intersects(boundingBox);
		};

		return rootRegion.getThisAndChildRegions(predicate);
	}

	/**
	 * Merge hops per node
	 * 
	 * @param regions
	 * @return
	 */
	private static Map<InetSocketAddress, RoutingHop> getHopListForRegion(
			final Map<List<DistributionRegion>, EnumSet<DistributionRegionHandlingFlag>> routingList) {
		
		final Map<InetSocketAddress, RoutingHop> hops = new HashMap<>();

		for(final Entry<List<DistributionRegion>, EnumSet<DistributionRegionHandlingFlag>> hop : routingList.entrySet()) {
			for(final DistributionRegion region : hop.getKey()) {
				for(final BBoxDBInstance system : region.getSystems()) {
	
					hops.computeIfAbsent(system.getInetSocketAddress(), 
							(i) -> new RoutingHop(system, new HashMap<>()))
						.addRegion(region.getRegionId(), hop.getValue());
				}
			}
		}
		
		return hops;
	}

	/**
	 * Build a list with hops
	 * @param knownInstances
	 * @param hops
	 * @return
	 */
	private static List<RoutingHop> removeUnavailableHops(final List<BBoxDBInstance> knownInstances,
			final Map<InetSocketAddress, RoutingHop> hops) {

		// No instances are known, this is this the case when a direct connection without
		// the membership instance manager is established.
		if(knownInstances.isEmpty()) {
			return new ArrayList<>(hops.values());
		}

		// Build a hashset with active connection points
		final Collection<InetSocketAddress> knownConnectionPoints = knownInstances.stream()
				.filter(i -> i.getState() == BBoxDBInstanceState.READY)
				.map(i -> i.getInetSocketAddress())
				.collect(Collectors.toCollection(HashSet::new));

		// Send data only to active instances
		final List<RoutingHop> hopList = hops.entrySet().stream()
				.filter(e -> knownConnectionPoints.contains(e.getKey()))
				.map(e -> e.getValue())
				.collect(Collectors.toList());

		if(logger.isDebugEnabled()) {
			logger.debug("Hop list is {}", hops.keySet());
		}

		if(hopList.isEmpty()) {
			logger.error("Hop list is empty hops {} / filtered hop list {} / known instances {}",
					hops, hopList, knownInstances);
		}

		return hopList;
	}
}

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
package org.bboxdb.network.routing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;

public class RoutingHopHelper {

	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public static List<RoutingHop> getRoutingHopsForRead(final DistributionRegion rootRegion,
			final Hyperrectangle boundingBox) {

		return getHopListForPredicateAndBox(rootRegion, boundingBox,
				DistributionRegionHelper.PREDICATE_REGIONS_FOR_READ);
	}

	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public static List<RoutingHop> getRoutingHopsForWrite(final DistributionRegion rootRegion,
			final Hyperrectangle boundingBox) {

		return getHopListForPredicateAndBox(rootRegion, boundingBox,
				DistributionRegionHelper.PREDICATE_REGIONS_FOR_WRITE);
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
			final Predicate<DistributionRegionState> statePredicate) {

		final Map<InetSocketAddress, RoutingHop> hops = new HashMap<>();

		final Predicate<DistributionRegion> predicate = (d) -> {
			return statePredicate.test(d.getState()) && d.getConveringBox().intersects(boundingBox);
		};

		final List<DistributionRegion> regions = rootRegion.getThisAndChildRegions(predicate);

		for(final DistributionRegion region : regions) {
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
}

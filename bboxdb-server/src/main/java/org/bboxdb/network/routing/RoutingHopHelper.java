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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.bboxdb.commons.Retryer;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.misc.Const;

public class RoutingHopHelper {

	/**
	 * Get a non empty routing list for write
	 * @param distributionRegion
	 * @param tuple
	 * @return
	 * @throws InterruptedException
	 */
	public static List<RoutingHop> getRoutingHopsForWriteWithRetry(final DistributionRegion distributionRegion,
			final BoundingBox boundingBox) throws InterruptedException {
		
		final Callable<List<RoutingHop>> getHops = new Callable<List<RoutingHop>>() {

			@Override
			public List<RoutingHop> call() throws Exception {
				final Collection<RoutingHop> hopCollection 
					= getRoutingHopsForWrite(distributionRegion, boundingBox);
				
				if(hopCollection.isEmpty()) {
					throw new Exception("Hop collection is empty");
				}

				return new ArrayList<>(hopCollection);
			}
		};
		
		final Retryer<List<RoutingHop>> retryer = new Retryer<>(Const.OPERATION_RETRY, 20, getHops);
		retryer.execute();
		return retryer.getResult();
	}
	
	/**
	 * Get a non empty routing list for read
	 * @param distributionRegion
	 * @param tuple
	 * @return
	 * @throws InterruptedException
	 */
	public static List<RoutingHop> getRoutingHopsForReadWithRetry(final DistributionRegion distributionRegion,
			final BoundingBox boundingBox) throws InterruptedException {
		
		final Callable<List<RoutingHop>> getHops = new Callable<List<RoutingHop>>() {

			@Override
			public List<RoutingHop> call() throws Exception {
				final Collection<RoutingHop> hopCollection 
					= getRoutingHopsForRead(distributionRegion, boundingBox);
				
				if(hopCollection.isEmpty()) {
					throw new Exception("Hop collection is empty");
				}

				return new ArrayList<>(hopCollection);
			}
		};
		
		final Retryer<List<RoutingHop>> retryer = new Retryer<>(Const.OPERATION_RETRY, 20, getHops);
		retryer.execute();
		return retryer.getResult();
	}
	

	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public static Collection<RoutingHop> getRoutingHopsForRead(final DistributionRegion rootRegion, 
			final BoundingBox boundingBox) {
		
		return getHopsForBoundingBoxRecursive(rootRegion, boundingBox, 
				DistributionRegionHelper.PREDICATE_REGIONS_FOR_READ);
	}
	
	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public static Collection<RoutingHop> getRoutingHopsForWrite(final DistributionRegion rootRegion, 
			final BoundingBox boundingBox) {
		
		return getHopsForBoundingBoxRecursive(rootRegion, boundingBox, 
				DistributionRegionHelper.PREDICATE_REGIONS_FOR_WRITE);		
	}
	
	/**
	 * Add the leaf nodes systems that are covered by the bounding box
	 * @param rootRegion 
	 * @param boundingBox
	 * @param systems
	 * @return 
	 */
	private static Collection<RoutingHop> getHopsForBoundingBoxRecursive(
			final DistributionRegion rootRegion, final BoundingBox boundingBox, 
			final Predicate<DistributionRegionState> statePredicate) {
	
		final Map<InetSocketAddress, RoutingHop> hops = new HashMap<>();
		
		final List<DistributionRegion> regions = rootRegion.getThisAndChildRegions();
		
		for(final DistributionRegion region : regions) {
			
			if(! boundingBox.overlaps(region.getConveringBox())) {
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
		
		return hops.values();
	}

}

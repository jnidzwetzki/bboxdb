/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.network.client.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.SpacePartitionerHelper;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.client.future.network.NetworkOperationFutureMultiImpl;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.routing.RoutingHopHelper;
import org.bboxdb.network.routing.DistributionRegionHandlingFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbtractClusterFutureBuilder {
	
	/**
	 * The distribution region
	 */
	private final DistributionRegion distributionRegion;
	
	/**
	 * The bounding box
	 */
	private final Hyperrectangle boundingBox;
	
	/**
	 * The routing options
	 */
	private EnumSet<DistributionRegionHandlingFlag> routingOptions;

	/**
	 * The membership connection service
	 */
	private final MembershipConnectionService membershipConnectionService;

	/**
	 * The cluster operation type
	 */
	private ClusterOperationType clusterOperationType;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AbtractClusterFutureBuilder.class);

	public AbtractClusterFutureBuilder(final ClusterOperationType clusterOperationType, 
			final String table, final Hyperrectangle boundingBox, final EnumSet<DistributionRegionHandlingFlag> routingOptions) throws BBoxDBException {
		
		this.clusterOperationType = clusterOperationType;
		this.routingOptions = routingOptions;
		this.distributionRegion = SpacePartitionerHelper.getRootNode(table);
		this.boundingBox = boundingBox;
		this.membershipConnectionService = MembershipConnectionService.getInstance();
	}

	public Supplier<List<NetworkOperationFuture>> getSupplier() {
		
		if(clusterOperationType == ClusterOperationType.READ_FROM_NODES_HA_IF_REPLICATED) {
			return getReplicatedSupplier();
		} else {
			return getUnreplicatedSupplier();
		}
	}

	/**
	 * Get the replicated supplier 
	 * Only one read operation per replicate needs to be successful
	 * 
	 * @return
	 */
	private Supplier<List<NetworkOperationFuture>> getReplicatedSupplier() {
		
		final Supplier<List<NetworkOperationFuture>> supplier = () -> {
			
			final List<NetworkOperationFuture> futures = new ArrayList<>();

			final List<DistributionRegion> regions = RoutingHopHelper.getRegionsForPredicate(
					distributionRegion, boundingBox, DistributionRegionHelper.PREDICATE_REGIONS_FOR_READ);
						
			if(regions.isEmpty()) {
				logger.error("Got empty hop list by bbox {} read {}", boundingBox, clusterOperationType);
			}

			for(final DistributionRegion region : regions) {
				final List<NetworkOperationFuture> futuresPerReplicate = new ArrayList<>();

				for(final BBoxDBInstance instance : region.getSystems()) {
					final BBoxDBConnection connection
						= membershipConnectionService.getConnectionForInstance(instance);
					
					// Node is down
					if(connection == null) {
						logger.debug("Skipping connection for {}", instance.getInetSocketAddress());
						continue;
					}
					
					final Map<Long, EnumSet<DistributionRegionHandlingFlag>> distributionRegions = new HashMap<>();
					distributionRegions.put(region.getRegionId(), routingOptions);
					
					final RoutingHop hop = new RoutingHop(instance, distributionRegions);

					final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));

					final Supplier<List<NetworkOperationFuture>> future = buildFuture(connection, routingHeader);
					futuresPerReplicate.addAll(future.get());
				}
				
				// Only one future of the list needs to be successful
				final NetworkOperationFutureMultiImpl future = new NetworkOperationFutureMultiImpl(
						futuresPerReplicate);
				
				futures.add(future);
			}

			return futures;
		};
		
		return supplier;
	}

	/**
	 * Get the unreplicated supplier
	 * All operations needs to be successful
	 * 
	 * @return
	 */
	private Supplier<List<NetworkOperationFuture>> getUnreplicatedSupplier() {
		final Supplier<List<NetworkOperationFuture>> supplier = () -> {
			
			final List<NetworkOperationFuture> futures = new ArrayList<>();

			final List<RoutingHop> hops = getHops();
			
			if(hops.isEmpty()) {
				logger.error("Got empty hop list by bbox {} read {}", boundingBox, clusterOperationType);
			}

			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();

				final BBoxDBConnection connection
					= membershipConnectionService.getConnectionForInstance(instance);

				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));

				final Supplier<List<NetworkOperationFuture>> future = buildFuture(connection, routingHeader);

				futures.addAll(future.get());
			}

			return futures;
		};
		return supplier;
	}
	
	/**
	 * Build the future
	 * @param routingHeader 
	 * @param connection 
	 * @return
	 */
	protected abstract Supplier<List<NetworkOperationFuture>> buildFuture(
			final BBoxDBConnection connection, final RoutingHeader routingHeader);
	
	/**
	 * Get the hop for the operation
	 * @return
	 */
	private List<RoutingHop> getHops() {
		switch(clusterOperationType) {
			case READ_FROM_NODES:
				return RoutingHopHelper.getRoutingHopsForRead(distributionRegion, boundingBox, routingOptions);
			case WRITE_TO_NODES:
				return RoutingHopHelper.getRoutingHopsForWrite(distributionRegion, boundingBox, routingOptions);
			default:
				throw new IllegalArgumentException("Unknown type: " + clusterOperationType);
		}
	}
}
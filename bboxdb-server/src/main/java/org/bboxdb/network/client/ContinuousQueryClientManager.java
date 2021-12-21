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

package org.bboxdb.network.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionEvent;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryClientManager {
	
	/**
	 * The query continuous plans
	 */
	private final Map<String, ContinuousQueryState> continousQueryStates;
	
	/**
	 * The running continuous queries
	 */
	private final Map<String, JoinedTupleListFuture> continousQueries;
	
	/**
	 * The known space partitioner
	 */
	private final Map<String, Set<ContinuousQueryState>> queryPlansPerDistributionRegion;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryClientManager.class);


	public ContinuousQueryClientManager() {
		this.continousQueryStates = new ConcurrentHashMap<>();
		this.continousQueries = new ConcurrentHashMap<>();
		this.queryPlansPerDistributionRegion = new HashMap<>();
	}

	/**
	 * Register the given query
	 * 
	 * @param queryUUID
	 * @param state
	 * @param future
	 * @throws BBoxDBException 
	 */
	public void registerQuery(final String queryUUID, 
			final ContinuousQueryState state, final JoinedTupleListFuture future) throws BBoxDBException {
		
		if(continousQueryStates.containsKey(queryUUID)) {
			throw new IllegalArgumentException("Query " + queryUUID + " is already registered");
		}
		
		continousQueryStates.put(queryUUID, state);
		continousQueries.put(queryUUID, future);
		
		registerReionCallbacks(state);
	}

	/**
	 * Register region callbacks
	 * @param state
	 * @throws BBoxDBException 
	 */
	private void registerReionCallbacks(final ContinuousQueryState state) throws BBoxDBException {
		
		final String distributionGroup = getDistributionGroupFromState(state);
		
		// Callbacks for distributin region are not registsered
		if(! queryPlansPerDistributionRegion.containsKey(distributionGroup)) {
			queryPlansPerDistributionRegion.put(distributionGroup, new HashSet<>());
			
			final SpacePartitioner spacePartitioner 
				= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroup);
		
			spacePartitioner.registerCallback((e, d) -> handleSpacePartitionerCallback(e, d));
		}
	
		queryPlansPerDistributionRegion.get(distributionGroup).add(state);
	}
	
	/**
	 * Get the distribution group from the state
	 * @param state
	 * @return
	 */
	private String getDistributionGroupFromState(final ContinuousQueryState state) {
		final String streamTable = state.getQueryPlan().getStreamTable();
		final TupleStoreName tupleStoreName = new TupleStoreName(streamTable);
		return tupleStoreName.getDistributionGroup();
	}
	
	/**
	 * Process the callbacks of the space partitioner
	 */
	public void handleSpacePartitionerCallback(final DistributionRegionEvent event, 
			final DistributionRegion distributionRegion) {
		
		if(event != DistributionRegionEvent.ADDED) {
			return;
		}
		
		final String groupName = distributionRegion.getDistributionGroupName();
		
		final Set<ContinuousQueryState> queryPlans = queryPlansPerDistributionRegion.get(groupName);
		
		// Register active queries on new region
		for(ContinuousQueryState state : queryPlans) {
			
			final ContinuousQueryPlan queryPlan = state.getQueryPlan();
			final Hyperrectangle queryRange = queryPlan.getQueryRange();
			
			if (queryRange.intersects(distributionRegion.getConveringBox())) {
				
				if(state.getRegisteredRegions().contains(distributionRegion.getRegionId())) {
					continue;
				}
				
				state.getRegisteredRegions().add(distributionRegion.getRegionId());
				
				final BBoxDBInstance firstSystem = distributionRegion.getSystems().get(0);

				final BBoxDBConnection connection = MembershipConnectionService.getInstance().getConnectionForInstance(firstSystem);

				final BBoxDBClient bboxDBClient = connection.getBboxDBClient();
				final Supplier<List<NetworkOperationFuture>> future = bboxDBClient.getQueryBoundingBoxContinousFuture(queryPlan);
				
				final JoinedTupleListFuture existingFutures = continousQueries.get(queryPlan.getQueryUUID());
				existingFutures.addFuture(future.get());				
			}
		}
	}
	
	/**
	 * Unregister the given query and return the query state
	 * 
	 * @param queryUUID
	 * @return
	 */
	public Optional<JoinedTupleListFuture> unregisterQuery(final String queryUUID) {
		
		if(! continousQueryStates.containsKey(queryUUID)) {
			return Optional.empty();
		}
		
		final JoinedTupleListFuture oldFuture = continousQueries.remove(queryUUID);
		final ContinuousQueryState oldState = continousQueryStates.remove(queryUUID);
		
		final String distributionGroup = getDistributionGroupFromState(oldState);
		final Set<ContinuousQueryState> activePlans = queryPlansPerDistributionRegion.get(distributionGroup);
		
		if(activePlans == null) {
			logger.error("Untable to deregister query, no plans for {} are known", distributionGroup);
		} else {
			activePlans.remove(oldState);
		}
		
		return Optional.of(oldFuture);
	}
	
	/**
	 * Get the query state
	 * @param queryUUID
	 * @return
	 */
	public Optional<ContinuousQueryState> getQueryState(final String queryUUID) {
		
		final ContinuousQueryState state = continousQueryStates.get(queryUUID);
		
		if(state == null) {
			return Optional.empty();
		}
		
		return Optional.of(state);
	}
}
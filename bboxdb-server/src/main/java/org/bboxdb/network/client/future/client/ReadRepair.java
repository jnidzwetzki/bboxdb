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
package org.bboxdb.network.client.future.client;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.connection.RoutingHeaderHelper;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.routing.DistributionRegionHandlingFlag;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadRepair {
	
	/**
	 * The tablename
	 */
	private final String tablename;

	/**
	 * The futures
	 */
	private List<NetworkOperationFuture> futures;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ReadRepair.class);

	public ReadRepair(final String tablename, final List<NetworkOperationFuture> futures) {
		this.tablename = tablename;
		this.futures = futures;
	}
	
	/**
	 * Perform read repair by checking all results
	 */
	public void performReadRepair(final List<Tuple> allTuples) {

		// Unable to perform read repair on only one result object
		if(futures.size() < 2) {
			return;
		}

		try {
			for(int resultId = 0; resultId < futures.size(); resultId++) {
				performReadRepairForResult(allTuples, resultId);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		} catch (BBoxDBException | ZookeeperException e) {
			logger.error("Got exception during read repair", e);
		}
	}

	/**
	 * Perform read repair for the given result
	 *
	 * @param allTuples
	 * @param resultId
	 * @throws InterruptedException
	 * @throws BBoxDBException
	 * @throws ZookeeperException
	 */
	private void performReadRepairForResult(final List<Tuple> allTuples, int resultId)
			throws InterruptedException, BBoxDBException, ZookeeperException {

		@SuppressWarnings("unchecked")
		final List<Tuple> tupleResult = (List<Tuple>) futures.get(resultId).get(true);
		final BBoxDBConnection bboxDBConnection = futures.get(resultId).getConnection();

		if(bboxDBConnection == null) {
			// Unable to perform read repair when the connection is not known
			return;
		}
		
		// Build max version map
		final Map<EntityIdentifier, Long> tupleMaxVersions = new HashMap<>();
		for(final Tuple tuple : allTuples) {
			
			final EntityIdentifier entityIdentifier = tuple.getEntityIdentifier();
			
			if(! tupleMaxVersions.containsKey(entityIdentifier)) {
				tupleMaxVersions.put(entityIdentifier, tuple.getVersionTimestamp());
			}
			
			if(tupleMaxVersions.get(entityIdentifier) < tuple.getVersionTimestamp()) {
				tupleMaxVersions.put(entityIdentifier, tuple.getVersionTimestamp());
			}
		}
		
		// Find outdated tuples
		final Set<EntityIdentifier> outdatedTuples = new HashSet<>();
		for(final Tuple tuple : allTuples) {
			final EntityIdentifier entityIdentifier = tuple.getEntityIdentifier();

			if(tupleMaxVersions.get(entityIdentifier) < tuple.getVersionTimestamp()) {
				outdatedTuples.add(entityIdentifier);
			}
		}

		for(final Tuple tuple : allTuples) {
			final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystem(
					tablename, tuple.getBoundingBox(), true, bboxDBConnection.getServerAddress(), true, EnumSet.noneOf(DistributionRegionHandlingFlag.class));

			// System is not responsible for the tuple
			if(routingHeader.getHopCount() == 0) {
				return;
			}

			final EntityIdentifier entityIdentifier = tuple.getEntityIdentifier();

			
			if(! tupleResult.contains(tuple)) {

				logger.info("Tuple {} is not contained in result {} from server {}, "
						+ "performing read repair", tuple, tupleResult,
						bboxDBConnection.getConnectionName());

				final BBoxDBClient bboxDBClient = bboxDBConnection.getBboxDBClient();
				bboxDBClient.insertTuple(tablename, tuple, routingHeader);
			}
			
			// Tuple is outdated on some nodes and we have the most recent version
			if(outdatedTuples.contains(entityIdentifier) && tupleMaxVersions.get(entityIdentifier) == tuple.getVersionTimestamp()) {
				logger.info("Performing read repair for tuple {}", entityIdentifier);
				final BBoxDBClient bboxDBClient = bboxDBConnection.getBboxDBClient();
				bboxDBClient.insertTuple(tablename, tuple, routingHeader);
			}
		}
	}
}

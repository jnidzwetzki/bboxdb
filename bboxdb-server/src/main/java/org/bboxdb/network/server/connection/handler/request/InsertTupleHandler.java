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
package org.bboxdb.network.server.connection.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.request.InsertTupleRequest;
import org.bboxdb.network.packets.response.ErrorResponse;
import org.bboxdb.network.routing.DistributionRegionHandlingFlag;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.bboxdb.network.server.query.ErrorMessages;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import io.prometheus.client.Gauge;

public class InsertTupleHandler implements RequestHandler {

	/**
	 * Should full stacktrace be included in the error message
	 */
	public static boolean includeStacktraceInError = true;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(InsertTupleHandler.class);
	
	/**
	 * The number of read insert packages
	 */
	private final static Gauge readInsertPacketsTotal = Gauge.build()
			.name("bboxdb_network_read_insert_packets_total")
			.help("Total amount read insert network packages").register();

	@Override
	/**
	 * Handle the insert tuple request
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage,
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler)
					throws IOException, PacketEncodeException {

		if(logger.isDebugEnabled()) {
			logger.debug("Got insert tuple request");
		}

		try {
			readInsertPacketsTotal.inc();
			final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage);

			// Does the tuple have the right dimension?
			final String distributionGroup = insertTupleRequest.getTable().getDistributionGroup();
			final DistributionGroupConfiguration groupConfiguration = DistributionGroupConfigurationCache
					.getInstance().getDistributionGroupConfiguration(distributionGroup);

			final Hyperrectangle boundingBox = insertTupleRequest.getTuple().getBoundingBox();

			if(! boundingBox.equals(Hyperrectangle.FULL_SPACE)) {
				final int groupDimensions = groupConfiguration.getDimensions();
				final int tupleDimensions = boundingBox.getDimension();

				if(groupDimensions != tupleDimensions) {
					final String errorMessage = ErrorMessages.ERROR_TUPLE_HAS_WRONG_DIMENSION
							+ " Group " + groupDimensions + " tuple " + tupleDimensions;
					final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
					clientConnectionHandler.writeResultPackage(responsePackage);
					return true;
				}
			}

			final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();

			if(! routingHeader.isRoutedPackage()) {
				final String errorMessage = ErrorMessages.ERROR_PACKAGE_NOT_ROUTED;
				logger.error(errorMessage);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}

			processPackageLocally(packageSequence, clientConnectionHandler, insertTupleRequest);

		} catch(RejectedException e) {
			final String errorMessage = buildErrorMessage(ErrorMessages.ERROR_LOCAL_OPERATION_REJECTED_RETRY, e);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
			clientConnectionHandler.writeResultPackage(responsePackage);
		} catch (Throwable e) {
			logger.error("Error while inserting tuple", e);
			final String errorMessage = buildErrorMessage(ErrorMessages.ERROR_EXCEPTION, e);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}

		return true;
	}

	/**
	 * Build the error message
	 * @param message
	 * @param e
	 * @return
	 */
	private String buildErrorMessage(final String message, final Throwable e) {
		final StringBuilder sb = new StringBuilder(message);

		sb.append(" ");
		sb.append(e.getMessage());

		if(includeStacktraceInError) {
			sb.append(" ");
			sb.append(Throwables.getStackTraceAsString(e));
		}

		return sb.toString();
	}

	/**
	 * @param packageSequence
	 * @param clientConnectionHandler
	 * @param insertTupleRequest
	 * @param routingHeader
	 * @throws BBoxDBException
	 * @throws RejectedException
	 * @throws PacketEncodeException
	 */
	private void processPackageLocally(final short packageSequence,
			final ClientConnectionHandler clientConnectionHandler,
			final InsertTupleRequest insertTupleRequest)
			throws BBoxDBException, RejectedException, PacketEncodeException {

		final Tuple tuple = insertTupleRequest.getTuple();
		final TupleStoreName requestTable = insertTupleRequest.getTable();
		final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();

		final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();
		final RoutingHop localHop = routingHeader.getRoutingHop();

		PackageRouter.checkLocalSystemNameMatchesAndThrowException(localHop);

		// Remove old locks
		final LockManager lockManager = clientConnectionHandler.getLockManager();
		final String table = insertTupleRequest.getTable().getFullnameWithoutPrefix();
		final String key = insertTupleRequest.getTuple().getKey();
		lockManager.removeLockForConnectionAndKey(clientConnectionHandler, table, key);

		final Map<Long, EnumSet<DistributionRegionHandlingFlag>> distributionRegions = localHop.getDistributionRegions();
		processInsertPackage(tuple, requestTable, storageRegistry, distributionRegions);
		forwardRoutedPackage(packageSequence, clientConnectionHandler, insertTupleRequest);
	}

	/**
	 * Forward the routed package
	 *
	 * @param packageSequence
	 * @param clientConnectionHandler
	 * @param insertTupleRequest
	 */
	private void forwardRoutedPackage(final short packageSequence,
			final ClientConnectionHandler clientConnectionHandler,
			final InsertTupleRequest insertTupleRequest) {

		final PackageRouter packageRouter = clientConnectionHandler.getPackageRouter();
		packageRouter.performInsertPackageRoutingAsync(packageSequence, insertTupleRequest);
	}

	/**
	 * Insert the table into the local storage
	 * @param tuple
	 * @param requestTable
	 * @param storageRegistry
	 * @param insertOptions 
	 * @param routingHeader
	 * @throws StorageManagerException
	 * @throws RejectedException
	 * @throws BBoxDBException
	 */
	protected void processInsertPackage(final Tuple tuple, final TupleStoreName requestTable,
			final TupleStoreManagerRegistry storageRegistry, 
			final Map<Long, EnumSet<DistributionRegionHandlingFlag>> distributionRegions) throws RejectedException {
		
		try {
			final String fullname = requestTable.getDistributionGroup();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache
					.getInstance().getSpacePartitionerForGroupName(fullname);

			final DistributionRegionIdMapper regionIdMapper = spacePartitioner
					.getDistributionRegionIdMapper();
			
			
			final Map<TupleStoreName, EnumSet<DistributionRegionHandlingFlag>> localTables = new HashMap<>();
			for(final Entry<Long, EnumSet<DistributionRegionHandlingFlag>> entry : distributionRegions.entrySet()) {
				localTables.put(requestTable.cloneWithDifferntRegionId(entry.getKey()), entry.getValue());
			}
			
			if(localTables.isEmpty()) {
				throw new BBoxDBException("Got no local tables for routed package");
			}

			// Are some tables unknown and needs to be created?
			TupleStoreManagerRegistryHelper.createMissingTables(requestTable, storageRegistry,
					localTables.keySet());

			// Insert tuples
			for(final Entry<TupleStoreName, EnumSet<DistributionRegionHandlingFlag>> localTable : localTables.entrySet()) {
				
				final long regionid = localTable.getKey().getRegionId().getAsLong();
				
				final Optional<Hyperrectangle> space 
					= regionIdMapper.getSpaceForRegionId(regionid);
				
				if(! space.isPresent()) {
					throw new IllegalArgumentException("Unable to get space for region: " + regionid);
				}
				
				final Hyperrectangle tupleBBox = tuple.getBoundingBox();
				final boolean storeOnDisk = ! localTable.getValue().contains(DistributionRegionHandlingFlag.STREAMING_ONLY);
				
				if(space.get().intersects(tupleBBox)) {
					final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(localTable.getKey());
					storageManager.put(tuple, storeOnDisk, true);
				} else { 
					logger.debug("Not inserting into region {} because {} not insertect {}", regionid, 
							tupleBBox, space);
				}
				
			}
		} catch (RejectedException e) {
			throw e;
		} catch (Throwable e) {
			throw new RejectedException(e);
		}
	}
}

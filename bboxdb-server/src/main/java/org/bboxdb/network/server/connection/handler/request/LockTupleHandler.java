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
package org.bboxdb.network.server.connection.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.LockTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockTupleHandler implements RequestHandler {

	/**
	 * There is no old version known
	 */
	public final static int NO_VERSION_KNOWN = -1;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(LockTupleHandler.class);

	@Override
	/**
	 * Lock the given tuple
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage,
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {

		try {
			final LockTupleRequest request = LockTupleRequest.decodeTuple(encodedPackage);
			final short sequenceNumber = request.getSequenceNumber();
			final String table = request.getTablename();
			final String key = request.getKey();
			final long version = request.getVersion();
			final boolean deleteOnTimeout = request.isDeleteOnTimeout();

			if(logger.isDebugEnabled()) {
				logger.debug("Locking tuple {} in table {} with version {}", key, table, version);
			}

			final long localVersion = getLocalTupleVersion(clientConnectionHandler, request);

			if(localVersion != version) {
				logger.info("Locking {} in table {} outdated. Local {} requested {}", key, table, localVersion, version);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_LOCK_FAILED_OUTDATED);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}

			final LockManager lockManager = clientConnectionHandler.getLockManager();
			final boolean lockResult = lockManager.lockTuple(clientConnectionHandler, sequenceNumber,
					table, key, version, deleteOnTimeout);

			if(lockResult == false) {
				logger.info("Lock tuple failed, pair {} / {} already locked", key, table);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_LOCK_FAILED_ALREADY_LOCKED);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}

			// Lock was successfully
			logger.debug("Lock for {} / {} was successfully", key, table);
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
			clientConnectionHandler.flushPendingCompressionPackages();
		} catch (Exception e) {
			logger.warn("Error while locking tuple", e);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
			clientConnectionHandler.flushPendingCompressionPackages();
		}

		return true;
	}

	/**
	 * Get the local tuple version
	 *
	 * @param clientConnectionHandler
	 * @param request
	 * @return
	 * @throws BBoxDBException
	 * @throws PackageEncodeException
	 * @throws StorageManagerException
	 */
	private long getLocalTupleVersion(final ClientConnectionHandler clientConnectionHandler,
			final LockTupleRequest request) throws BBoxDBException, PackageEncodeException, StorageManagerException {

		final List<Tuple> tuplesForKey = getAllTuplesForKey(clientConnectionHandler, request);

		return tuplesForKey
				.stream()
				.mapToLong(t -> t.getVersionTimestamp())
				.max()
				.orElse(NO_VERSION_KNOWN);
	}

	/**
	 * Get all tuples for the given key
	 *
	 * @param clientConnectionHandler
	 * @param request
	 * @param table
	 * @param key
	 * @return
	 * @throws BBoxDBException
	 * @throws PackageEncodeException
	 * @throws StorageManagerException
	 */
	private List<Tuple> getAllTuplesForKey(final ClientConnectionHandler clientConnectionHandler,
			final LockTupleRequest request) throws BBoxDBException, PackageEncodeException,
			StorageManagerException {

		final String table = request.getTablename();
		final String key = request.getKey();

		final TupleStoreName requestTable = new TupleStoreName(table);
		final DistributionRegionIdMapper regionIdMapper = getRegionIdMapper(requestTable);

		final RoutingHeader routingHeader = request.getRoutingHeader();
		final RoutingHop localHop = routingHeader.getRoutingHop();

		PackageRouter.checkLocalSystemNameMatchesAndThrowException(localHop);

		final List<Long> distributionRegions = localHop.getDistributionRegions();
		final List<Tuple> tuplesForKey = new ArrayList<>();

		final Collection<TupleStoreName> localTables = regionIdMapper.convertRegionIdToTableNames(
				requestTable, distributionRegions);

		for(final TupleStoreName tupleStoreName : localTables) {

			final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler
					.getStorageRegistry();

			// Ignore non existing tables
			if(! storageRegistry.isStorageManagerKnown(tupleStoreName)) {
				continue;
			}

			final TupleStoreManager storageManager = storageRegistry
					.getTupleStoreManager(tupleStoreName);

			final List<Tuple> tuplesInTable = storageManager.get(key);
			tuplesForKey.addAll(tuplesInTable);
		}

		return tuplesForKey;
	}

	/**
	 * Get the region ID mapper
	 * @param requestTable
	 * @return
	 * @throws BBoxDBException
	 */
	private DistributionRegionIdMapper getRegionIdMapper(final TupleStoreName requestTable) throws BBoxDBException {
		final String fullname = requestTable.getDistributionGroup();
		final SpacePartitioner spacePartitioner = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(fullname);

		return spacePartitioner.getDistributionRegionIdMapper();
	}
}

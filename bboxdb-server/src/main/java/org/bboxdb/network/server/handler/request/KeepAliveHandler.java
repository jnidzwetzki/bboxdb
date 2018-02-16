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
package org.bboxdb.network.server.handler.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.DistributionRegionIdMapperManager;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeepAliveHandler implements RequestHandler {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KeepAliveHandler.class);

	/**
	 * Handle the keep alive package. Simply send a success response package back
	 */
	@Override
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {

		final KeepAliveRequest keepAliveRequst = KeepAliveRequest.decodeTuple(encodedPackage);

		boolean gossipResult = true;
		if(! keepAliveRequst.getTuples().isEmpty()) {
			gossipResult = handleGossip(keepAliveRequst, clientConnectionHandler);
		}

		if(gossipResult) {
			final SuccessResponse responsePackage = new SuccessResponse(packageSequence);
			clientConnectionHandler.writeResultPackage(responsePackage);
		} else {
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, 
					ErrorMessages.ERROR_OUTDATED_TUPLES);

			clientConnectionHandler.writeResultPackage(responsePackage);
		}

		return true;
	}

	/**
	 * Handle keep alive gossip
	 * @param keepAliveRequst
	 * @param clientConnectionHandler 
	 * @return 
	 */
	private boolean handleGossip(final KeepAliveRequest keepAliveRequst, 
			final ClientConnectionHandler clientConnectionHandler) {

		final String table = keepAliveRequst.getTablename();
		final TupleStoreName tupleStoreName = new TupleStoreName(table);
		final List<Tuple> tuples = keepAliveRequst.getTuples();
		final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();

		for(final Tuple tuple : tuples) {
			final boolean result = checkLocalTuples(storageRegistry, tupleStoreName, tuple);

			if(! result) {
				return false;
			}
		}

		return true;
	}

	/**
	 * @param tupleStoreManagerRegistry
	 * @param tupleStoreName
	 * @param tuple
	 * @throws StorageManagerException
	 */
	private boolean checkLocalTuples(final TupleStoreManagerRegistry tupleStoreManagerRegistry,
			final TupleStoreName tupleStoreName, final Tuple tuple) {

		final DistributionRegionIdMapper regionIdMapper = DistributionRegionIdMapperManager.getInstance(tupleStoreName.getDistributionGroupObject());

		final Collection<TupleStoreName> localTables = regionIdMapper.getLocalTablesForRegion(tuple.getBoundingBox(), tupleStoreName);

		for(final TupleStoreName localTupleStoreName : localTables) {
			try {
				final TupleStoreManager storageManager = tupleStoreManagerRegistry.getTupleStoreManager(localTupleStoreName);    
				final String key = tuple.getKey();
				final List<Tuple> localTuples = storageManager.get(key);

				if(localTables.isEmpty()) {
					logger.error("Got empty tuple list during gossip");
					return false;
				}

				final List<Long> localVersions = getSortedVersionList(localTuples);
				final long gossipTupleVersion = tuple.getVersionTimestamp();

				return checkLocalTupleVersions(localVersions, gossipTupleVersion, key);
			} catch (StorageManagerException e) {
				logger.error("Got exception while reading tuples", e);
			}
		}

		return true;
	}

	/**
	 * Check the local tuple versions
	 * @param localVersions
	 * @param gossipTupleVersion
	 * @param key 
	 * @return 
	 */
	private boolean checkLocalTupleVersions(final List<Long> localVersions, 
			final long gossipTupleVersion, final String key) {

		if(localVersions.isEmpty()) {
			logger.error("Gossip: no local version known for {} / gossip: {}", 
					key, gossipTupleVersion);
			
			return false;
		}
		
		if(gossipTupleVersion > localVersions.get(0)) {
			logger.error("Gossip: Remote knows a newer version {} / local {} for {}", 
					gossipTupleVersion, localVersions, key);
			
			return false;
		}
		
		if(! localVersions.contains(gossipTupleVersion)) {
			logger.error("Gossip: Tuple version {} is not contained in list {} for {}", 
					gossipTupleVersion, localVersions, key);

			return false;
		}
		
		logger.debug("Gossip: Remote version {} / local {} for key {}", 
				gossipTupleVersion, localVersions, key);
		
		return true;
	}

	/**
	 * Get a list with all versions
	 * 
	 * @param localTuples
	 * @return
	 */
	private List<Long> getSortedVersionList(final List<Tuple> localTuples) {
		// Get local tuple versions (sorted)
		final List<Long> localVersions = localTuples.stream()
				.mapToLong(t -> t.getVersionTimestamp())
				.sorted()
				.boxed()
				.collect(Collectors.toList());
		return localVersions;
	}
}

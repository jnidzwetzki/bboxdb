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

import org.bboxdb.commons.RejectedException;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertTupleHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(InsertTupleHandler.class);
	

	@Override
	/**
	 * Handle the insert tuple request
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Got insert tuple request");
		}
		
		try {			
			final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage);
			
			final Tuple tuple = insertTupleRequest.getTuple();			
			final TupleStoreName requestTable = insertTupleRequest.getTable();
			final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();
			final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();
			
			if(! routingHeader.isRoutedPackage()) {
				final String errorMessage = "Error while insering tuple - package is not routed";
				logger.error(errorMessage);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
				clientConnectionHandler.writeResultPackage(responsePackage);
			} else {
				handleRoutedPackage(tuple, requestTable, storageRegistry, routingHeader);
				final PackageRouter packageRouter = clientConnectionHandler.getPackageRouter();
				packageRouter.performInsertPackageRoutingAsync(packageSequence, insertTupleRequest);
			}
			
		} catch (Exception e) {
			logger.error("Error while inserting tuple", e);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);	
		}
		
		return true;
	}

	/**
	 * Insert the table into the local storage
	 * @param tuple
	 * @param requestTable
	 * @param storageRegistry
	 * @param routingHeader
	 * @throws StorageManagerException
	 * @throws RejectedException
	 * @throws BBoxDBException
	 */
	protected void handleRoutedPackage(final Tuple tuple, final TupleStoreName requestTable, 
			final TupleStoreManagerRegistry storageRegistry,
			final RoutingHeader routingHeader) 
			throws StorageManagerException, RejectedException, BBoxDBException {
		
		final RoutingHop localHop = routingHeader.getRoutingHop();
		
		PackageRouter.checkLocalSystemNameMatches(localHop);
		
		final DistributionGroupName distributionGroupObject = requestTable.getDistributionGroupObject();
		
		final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(distributionGroupObject);

		final Collection<TupleStoreName> localTables = regionIdMapper.convertRegionIdToTableNames(
					requestTable, localHop.getDistributionRegions());

		// Are some tables unknown and needs to be created?
		final boolean unknownTables = localTables.stream()
			.anyMatch((t) -> ! storageRegistry.isStorageManagerKnown(t)); 
		
		// Expensive call (involves Zookeeper interaction)
		if(unknownTables) {
			createMissingTables(requestTable, storageRegistry, localTables);
		}
		
		// Insert tuples
		for(final TupleStoreName tupleStoreName : localTables) {
			final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(tupleStoreName);
			storageManager.put(tuple);			
		}
	}

	/**
	 * Create all miising tables
	 */
	protected void createMissingTables(final TupleStoreName requestTable,
			final TupleStoreManagerRegistry storageRegistry, final Collection<TupleStoreName> localTables)
			throws StorageManagerException {
		
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final TupleStoreAdapter tupleStoreAdapter = new TupleStoreAdapter(zookeeperClient);
			final TupleStoreConfiguration config = tupleStoreAdapter.readTuplestoreConfiguration(requestTable);

			for(final TupleStoreName tupleStoreName : localTables) {
				final boolean alreadyKnown = storageRegistry.isStorageManagerKnown(tupleStoreName);
				
				if(! alreadyKnown) {
					storageRegistry.createTable(tupleStoreName, config);
				}
			}
		} catch (ZookeeperException e) {
			throw new StorageManagerException(e);
		}
	}

}

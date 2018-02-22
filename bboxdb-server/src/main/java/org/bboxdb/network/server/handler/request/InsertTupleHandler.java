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

import org.bboxdb.commons.RejectedException;
import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
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
			
			final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();
	
			if(! routingHeader.isRoutedPackage()) {
				final String errorMessage = "Error while inserting tuple - package is not routed";
				logger.error(errorMessage);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			} 
			
			// Needs to be rerouted?
			if(routingHeader.getHop() == -1) {
				routingHeader.dispatchToNextHop();
				final RoutingHop localHop = routingHeader.getRoutingHop();
				
				if(PackageRouter.checkLocalSystemNameMatches(localHop)) {
					processPackageLocally(packageSequence, clientConnectionHandler, insertTupleRequest);
				} else {
					logger.debug("Rerouting package {}", packageSequence);
					forwardRoutedPackage(packageSequence, clientConnectionHandler, insertTupleRequest);
				}
			} else {
				processPackageLocally(packageSequence, clientConnectionHandler, insertTupleRequest);
			}
			
		} catch(RejectedException e) {
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, 
					ErrorMessages.ERROR_LOCAL_OPERATION_REJECTED_RETRY + " " + e.getMessage());
			clientConnectionHandler.writeResultPackage(responsePackage);	
		} catch (Throwable e) {
			logger.error("Error while inserting tuple", e);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);	
		}
		
		return true;
	}

	/**
	 * @param packageSequence
	 * @param clientConnectionHandler
	 * @param insertTupleRequest
	 * @param routingHeader
	 * @throws BBoxDBException
	 * @throws RejectedException
	 * @throws PackageEncodeException 
	 */
	private void processPackageLocally(final short packageSequence,
			final ClientConnectionHandler clientConnectionHandler, 
			final InsertTupleRequest insertTupleRequest) 
			throws BBoxDBException, RejectedException, PackageEncodeException {
		
		final Tuple tuple = insertTupleRequest.getTuple();			
		final TupleStoreName requestTable = insertTupleRequest.getTable();
		final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();
		
		final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();
		final RoutingHop localHop = routingHeader.getRoutingHop();
		
		PackageRouter.checkLocalSystemNameMatchesAndThrowException(localHop);		
		
		final List<Long> distributionRegions = localHop.getDistributionRegions();
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
	 * @param routingHeader
	 * @throws StorageManagerException
	 * @throws RejectedException
	 * @throws BBoxDBException
	 */
	protected void processInsertPackage(final Tuple tuple, final TupleStoreName requestTable, 
			final TupleStoreManagerRegistry storageRegistry, final List<Long> distributionRegions) throws RejectedException {
		
		try {
			
			final String fullname = requestTable.getDistributionGroup();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getSpacePartitionerForGroupName(fullname);
			final DistributionRegionIdMapper regionIdMapper = spacePartitioner.getDistributionRegionIdMapper();

			final Collection<TupleStoreName> localTables = regionIdMapper.convertRegionIdToTableNames(
						requestTable, distributionRegions);
			
			if(localTables.isEmpty()) {
				throw new BBoxDBException("Got no local tables for routed package");
			}
						
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
		} catch (RejectedException e) {
			throw e;
		} catch (Throwable e) {
			throw new RejectedException(e);
		} 
	}

	/**
	 * Create all missing tables
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
					storageRegistry.createTableIfNotExist(tupleStoreName, config);
				}
			}
		} catch (ZookeeperException e) {
			throw new StorageManagerException(e);
		}
	}

}

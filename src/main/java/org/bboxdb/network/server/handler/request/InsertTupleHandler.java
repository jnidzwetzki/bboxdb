/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
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
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.registry.StorageRegistry;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.util.RejectedException;
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
			final SSTableName requestTable = insertTupleRequest.getTable();
			final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();
			final StorageRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();
			
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
	protected void handleRoutedPackage(final Tuple tuple, final SSTableName requestTable, 
			final StorageRegistry storageRegistry,
			final RoutingHeader routingHeader) 
			throws StorageManagerException, RejectedException, BBoxDBException {
		
		final RoutingHop localHop = routingHeader.getRoutingHop();
		
		checkSystemNameMatches(localHop);
		
		final DistributionGroupName distributionGroupObject = requestTable.getDistributionGroupObject();
		
		final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(distributionGroupObject);

		final Collection<SSTableName> localTables = regionIdMapper.convertRegionIdToTableNames(
					requestTable, localHop.getDistributionRegions());

		for(final SSTableName ssTableName : localTables) {
			final SSTableManager storageManager = storageRegistry.getSSTableManager(ssTableName);
			storageManager.put(tuple);			
		}
	}

	/**
	 * Ensure that the package is routed to the right system
	 * @param localHop
	 * @throws BBoxDBException
	 */
	protected void checkSystemNameMatches(final RoutingHop localHop) throws BBoxDBException {
		final DistributedInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName();
		final DistributedInstance routingInstanceName = localHop.getDistributedInstance();
		
		if(! localInstanceName.socketAddressEquals(routingInstanceName)) {
			throw new BBoxDBException("Routing hop " + routingInstanceName 
					+ " does not match local host " + localInstanceName);
		}
	}
}

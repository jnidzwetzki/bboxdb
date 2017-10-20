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
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.DeleteTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTupleHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DeleteTupleHandler.class);
	

	@Override
	/**
	 * Handle delete tuple package
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		
		try {
			final DeleteTupleRequest deleteTupleRequest = DeleteTupleRequest.decodeTuple(encodedPackage);
			final TupleStoreName requestTable = deleteTupleRequest.getTable();
			final RoutingHeader routingHeader = deleteTupleRequest.getRoutingHeader();

			if(! routingHeader.isRoutedPackage()) {
				final String errorMessage = "Error while deleting tuple - package is not routed";
				logger.error(errorMessage);
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
				clientConnectionHandler.writeResultPackage(responsePackage);
			} else {
				final RoutingHop localHop = routingHeader.getRoutingHop();
				
				PackageRouter.checkLocalSystemNameMatches(localHop);
				
				final DistributionGroupName distributionGroupObject = requestTable.getDistributionGroupObject();
				
				final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(distributionGroupObject);

				final Collection<TupleStoreName> localTables = regionIdMapper.convertRegionIdToTableNames(
							requestTable, localHop.getDistributionRegions());

				for(final TupleStoreName ssTableName : localTables) {
					final TupleStoreManager storageManager = clientConnectionHandler
							.getStorageRegistry()
							.getSSTableManager(ssTableName);
					
					storageManager.delete(deleteTupleRequest.getKey(), deleteTupleRequest.getTimestamp());
				}
			}

			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while delete tuple", e);

			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		} 

		return true;
	}
}

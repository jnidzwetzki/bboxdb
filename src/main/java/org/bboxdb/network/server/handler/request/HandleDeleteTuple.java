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

import java.nio.ByteBuffer;
import java.util.Collection;

import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.DeleteTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleDeleteTuple implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleDeleteTuple.class);
	

	@Override
	/**
	 * Handle delete tuple package
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Got delete tuple package");
		}
		
		try {
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				final ErrorResponse errorResponse = new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY);
				clientConnectionHandler.writeResultPackage(errorResponse);
				return true;
			}
			
			final DeleteTupleRequest deleteTupleRequest = DeleteTupleRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = deleteTupleRequest.getTable();

			// Send the call to the storage manager
			final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final Collection<SSTableName> localTables = regionIdMapper.getAllLocalTables(requestTable);

			for(final SSTableName ssTableName : localTables) {
				final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
				storageManager.delete(deleteTupleRequest.getKey(), deleteTupleRequest.getTimestamp());
			}
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		} catch (PackageEncodeException e) {
			logger.warn("Error while delete tuple", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		} 

		return true;
	}
}

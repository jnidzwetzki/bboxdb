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
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleInsertTuple implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleInsertTuple.class);
	

	@Override
	/**
	 * Handle the insert tuple request
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Got insert tuple request");
		}
		
		try {
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}
			
			final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage);
			
			// Send the call to the storage manager
			final Tuple tuple = insertTupleRequest.getTuple();			
			final SSTableName requestTable = insertTupleRequest.getTable();
			
			final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final BoundingBox boundingBox = insertTupleRequest.getTuple().getBoundingBox();
			final Collection<SSTableName> localTables = regionIdMapper.getLocalTablesForRegion(boundingBox, requestTable);

			for(final SSTableName ssTableName : localTables) {	
				insertTupleNE(tuple, ssTableName);
			}

			clientConnectionHandler.getPackageRouter().performInsertPackageRoutingAsync(packageSequence, insertTupleRequest, boundingBox);
			
		} catch (Exception e) {
			logger.warn("Error while insert tuple", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}
		
		return true;
	}


	/**
	 * Insert an tuple with proper exception handling
	 * @param tuple
	 * @param ssTableName
	 * @return
	 */
	protected boolean insertTupleNE(final Tuple tuple, final SSTableName ssTableName) {
		
		SSTableManager storageManager = null;
		
		try {
			storageManager = StorageRegistry.getSSTableManager(ssTableName);
		} catch (StorageManagerException e) {
			logger.warn("Got an exception while inserting", e);
			return false;
		}
	
		try {
			if(! storageManager.isReady()) {
				return false;
			}
			
			storageManager.put(tuple);
			return true;
			
		} catch (StorageManagerException e) {
			if(storageManager.isReady()) {
				logger.warn("Got an exception while inserting", e);
			} else {
				logger.debug("Got an exception while inserting", e);
			}
		}
		
		return false;
	}
	
}

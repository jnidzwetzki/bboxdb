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

import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CreateTableRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTableHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CreateTableHandler.class);
	

	@Override
	/**
	 * Handle the create table call
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		
		try {			
			final CreateTableRequest createPackage = CreateTableRequest.decodeTuple(encodedPackage);
			final TupleStoreName requestTable = createPackage.getTable();
			logger.info("Got create call for table: {}", requestTable.getFullname());
			
			if(! requestTable.isValid()) {
				logger.warn("Got invalid table name {}", requestTable);
				returnWithError(packageSequence, clientConnectionHandler, ErrorMessages.ERROR_TABLE_INVALID_NAME);
				return true;
			}
			
			final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
					.getZookeeperClient().getTupleStoreAdapter();
			
			if(tupleStoreAdapter.isTableKnown(requestTable)) {
				logger.warn("Table name is already known {}", requestTable.getFullname());
				returnWithError(packageSequence, clientConnectionHandler, ErrorMessages.ERROR_TABLE_EXISTS);
				return true;
			} else {
				tupleStoreAdapter.writeTuplestoreConfiguration(requestTable, 
						createPackage.getTupleStoreConfiguration());
				
				clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
			}
		} catch (Exception e) {
			logger.warn("Error while delete tuple", e);

			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}

	/** 
	 * Return call with a error
	 * @param packageSequence
	 * @param clientConnectionHandler
	 * @param errorMessage
	 * @throws IOException
	 * @throws PackageEncodeException
	 */
	private void returnWithError(final short packageSequence, 
			final ClientConnectionHandler clientConnectionHandler, final String errorMessage)
			throws IOException, PackageEncodeException {
		
		final ErrorResponse responsePackage = new ErrorResponse(packageSequence, errorMessage);
		clientConnectionHandler.writeResultPackage(responsePackage);
	}
}

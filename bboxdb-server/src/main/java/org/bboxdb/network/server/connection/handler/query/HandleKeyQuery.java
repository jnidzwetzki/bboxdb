/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.network.server.connection.handler.query;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.request.QueryKeyRequest;
import org.bboxdb.network.packets.response.ErrorResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.query.ClientQuery;
import org.bboxdb.network.server.query.ErrorMessages;
import org.bboxdb.network.server.query.KeyClientQuery;
import org.bboxdb.network.server.query.QueryHelper;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleKeyQuery implements QueryHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandleKeyQuery.class);
	

	@Override
	/**
	 * Handle a key query
	 */
	public void handleQuery(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PacketEncodeException {

		final Runnable queryRunable = new ExceptionSafeRunnable() {

			@Override
			public void runThread() throws Exception {
				
				try {	
					if(clientConnectionHandler.getActiveQueries().containsKey(packageSequence)) {
						logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
						return;
					}
					
					final QueryKeyRequest queryKeyRequest = QueryKeyRequest.decodeTuple(encodedPackage);
					final TupleStoreName requestTable = queryKeyRequest.getTable();
					
					if(! QueryHelper.handleNonExstingTable(requestTable, packageSequence, clientConnectionHandler)) {
						return;
					}
					
					final String key = queryKeyRequest.getKey();
					
					final ClientQuery clientQuery = new KeyClientQuery(key, queryKeyRequest.isPagingEnabled(), 
							queryKeyRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, requestTable);
					
					clientConnectionHandler.getActiveQueries().put(packageSequence, clientQuery);
					clientConnectionHandler.sendNextResultsForQuery(packageSequence, packageSequence);
				} catch (PacketEncodeException e) {
					logger.warn("Got exception while decoding package", e);
					clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
				}
			}			
			
			@Override
			protected void afterExceptionHook() {
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
				clientConnectionHandler.writeResultPackageNE(responsePackage);	
			}
		};

		// Submit the runnable to our pool
		if(clientConnectionHandler.getThreadPool().isShutdown()) {
			logger.warn("Thread pool is shutting down, don't execute query: {}", packageSequence);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_SHUTDOWN);
			clientConnectionHandler.writeResultPackage(responsePackage);
		} else {
			clientConnectionHandler.getThreadPool().submit(queryRunable);
		}		
	}
}

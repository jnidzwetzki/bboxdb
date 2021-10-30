/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
import java.util.List;
import java.util.Map;

import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.packets.request.CancelRequest;
import org.bboxdb.network.packets.response.ErrorResponse;
import org.bboxdb.network.packets.response.SuccessResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.connection.lock.LockEntry;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.bboxdb.network.server.query.ClientQuery;
import org.bboxdb.network.server.query.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CancelRequestHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CancelRequestHandler.class);

	@Override
	/**
	 * Cancel the given query
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PacketEncodeException {
		
		try {
			final CancelRequest cancelPackage = CancelRequest.decodeTuple(encodedPackage);
			final short queryToCancel = cancelPackage.getQuerySequence();
			
			final Map<Short, ClientQuery> activeQueries = clientConnectionHandler.getActiveQueries();
			
			final ClientQuery clientQuery = activeQueries.get(queryToCancel);

			if(clientQuery != null) {
				clientQuery.close();
			} else {
				logger.error("Unable to cancel client query {}, it does not exists", queryToCancel);
			}
			
			removeLocks(packageSequence, clientConnectionHandler, queryToCancel);
			
			logger.info("Sending success for canceling query {} (request package {})", 
					queryToCancel, packageSequence);
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (PacketEncodeException e) {
			logger.warn("Error getting next page for a query", e);

			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}

	/**
	 * Remove the locks for the given sequence
	 * @param packageSequence
	 * @param clientConnectionHandler
	 * @param requestPackage
	 * @throws IOException
	 * @throws PacketEncodeException
	 */
	private void removeLocks(final short packageSequence, final ClientConnectionHandler clientConnectionHandler,
			final short requestPackage) throws IOException, PacketEncodeException {
		
		final LockManager lockManager = clientConnectionHandler.getLockManager();
		
		final List<LockEntry> removedLocks = lockManager.removeAllForLocksForObjectAndSequence(
				clientConnectionHandler, requestPackage);
		
		logger.info("Removed {} locks for query {}", removedLocks.size(), requestPackage);
	}
}

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
import java.util.Map;

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CancelQueryRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ClientQuery;
import org.bboxdb.network.server.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CancelQueryHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CancelQueryHandler.class);

	@Override
	/**
	 * Cancel the given query
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {
		
		try {
			final CancelQueryRequest nextPagePackage = CancelQueryRequest.decodeTuple(encodedPackage);
			logger.debug("Cancel query {} requested", nextPagePackage.getQuerySequence());
			
			final Map<Short, ClientQuery> activeQueries = clientConnectionHandler.getActiveQueries();
			
			if(! activeQueries.containsKey(packageSequence)) {
				logger.error("Unable to cancel query {} - not found", packageSequence);
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_NOT_FOUND));
			} else {
				final ClientQuery clientQuery = activeQueries.remove(packageSequence);
				clientQuery.close();
				clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
			}
		} catch (PackageEncodeException e) {
			logger.warn("Error getting next page for a query", e);

			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}
}

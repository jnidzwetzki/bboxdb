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

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NextPageHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NextPageHandler.class);
	

	@Override
	/**
	 * Handle the next page package
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		try {
			final NextPageRequest nextPagePackage = NextPageRequest.decodeTuple(encodedPackage);
			logger.debug("Next page for query {} called", nextPagePackage.getQuerySequence());
			
			// Send tuples as result for original query
			clientConnectionHandler.sendNextResultsForQuery(packageSequence, nextPagePackage.getQuerySequence());

		} catch (PackageEncodeException e) {
			logger.warn("Error getting next page for a query", e);
			final ErrorResponse errorResponse = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(errorResponse);	
		}
		
		return true;
		
	}
	
	
}

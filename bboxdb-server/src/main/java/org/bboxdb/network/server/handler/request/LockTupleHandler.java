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
import org.bboxdb.network.packages.request.LockTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockTupleHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(LockTupleHandler.class);

	@Override
	/**
	 * Lock the given tuple
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {
		
		try {
			final LockTupleRequest request = LockTupleRequest.decodeTuple(encodedPackage);
			final String table = request.getTablename();
			final String key = request.getKey();
			final long version = request.getVersion();
			
			logger.debug("Lock tuple {}, {}, {} requested", table, key, version);
			
			
		} catch (PackageEncodeException e) {
			logger.warn("Error while locing tuple", e);

			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
		}
		
		return true;
	}
}

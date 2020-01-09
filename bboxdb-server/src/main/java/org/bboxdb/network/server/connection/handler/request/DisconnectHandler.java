/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisconnectHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DisconnectHandler.class);
	

	@Override
	/**
	 * Handle the disconnect request
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) 
					throws IOException, PackageEncodeException {
		
		logger.info("Got disconnect package, preparing for connection close: "  
				+ clientConnectionHandler.clientSocket.getInetAddress());
		
		clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		return false;
	}
}

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

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandshakeHandler implements RequestHandler {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(HandshakeHandler.class);
	

	@Override
	/**
	 * Handle the handshake request
	 */
	public boolean handleRequest(final ByteBuffer encodedPackage, 
			final short packageSequence, final ClientConnectionHandler clientConnectionHandler) throws IOException, PackageEncodeException {
		
		logger.info("Handshaking with: " + clientConnectionHandler.clientSocket.getInetAddress());
		
		try {	
			final HelloRequest heloRequest = HelloRequest.decodeRequest(encodedPackage);
			clientConnectionHandler.setConnectionCapabilities(heloRequest.getPeerCapabilities());

			final HelloResponse responsePackage = new HelloResponse(packageSequence, NetworkConst.PROTOCOL_VERSION, clientConnectionHandler.getConnectionCapabilities());
			clientConnectionHandler.writeResultPackage(responsePackage);

			clientConnectionHandler.setConnectionStateToOpen();
			return true;
		} catch(Exception e) {
			logger.warn("Error while reading network package", e);

			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
			return false;
		}
	}
}

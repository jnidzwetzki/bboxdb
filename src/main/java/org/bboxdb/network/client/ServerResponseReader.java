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
package org.bboxdb.network.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.util.ExceptionSafeThread;
import org.bboxdb.util.StreamHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read the server response packages
 *
 */
public class ServerResponseReader extends ExceptionSafeThread {
	
	/**
	 * The BBOXDB Client
	 */
	protected final BBoxDBClient bboxDBClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ServerResponseReader.class);
	
	public ServerResponseReader(final BBoxDBClient bboxDBClient) {
		this.bboxDBClient = bboxDBClient;
	}
	
	/**
	 * Read the next response package header from the server
	 * @return 
	 * @throws IOException 
	 */
	protected ByteBuffer readNextResponsePackageHeader(final InputStream inputStream) throws IOException {
		final ByteBuffer bb = ByteBuffer.allocate(12);
		StreamHelper.readExactlyBytes(inputStream, bb.array(), 0, bb.limit());
		return bb;
	}
	
	/**
	 * Process the next server answer
	 */
	protected boolean processNextResponsePackage(final InputStream inputStream) {
		try {
			final ByteBuffer bb = readNextResponsePackageHeader(inputStream);
			
			if(bb == null) {
				// Ignore exceptions when connection is closing
				if(bboxDBClient.connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
					logger.error("Read error from socket, exiting");
				}
				return false;
			}
			
			final ByteBuffer encodedPackage = bboxDBClient.readFullPackage(bb, inputStream);
			bboxDBClient.handleResultPackage(encodedPackage);
			
		} catch (IOException | PackageEncodeException e) {
			
			// Ignore exceptions when connection is closing
			if(bboxDBClient.connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				logger.error("Unable to read data from server (state: " + bboxDBClient.connectionState + ")", e);
				return false;
			}
		}
		
		return true;
	}

	/**
	 * Kill pending calls
	 */
	protected void handleSocketClosedUnexpected() {
		bboxDBClient.connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED_WITH_ERRORS;
		bboxDBClient.killPendingCalls();
	}

	@Override
	protected void beginHook() {
		logger.info("Started new response reader for " + bboxDBClient.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.info("Stopping new response reader for " + bboxDBClient.getConnectionName());
	}
	
	@Override
	public void runThread() {
		
		while(bboxDBClient.clientSocket != null) {
			boolean result = processNextResponsePackage(bboxDBClient.inputStream);
			
			if(result == false) {
				handleSocketClosedUnexpected();
				break;
			}
		}
		
	}
}

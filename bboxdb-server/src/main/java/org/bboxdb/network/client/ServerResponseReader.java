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
package org.bboxdb.network.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.network.packages.PackageEncodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

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
		ByteStreams.readFully(inputStream, bb.array(), 0, bb.limit());
		return bb;
	}
	
	/**
	 * Process the next server answer
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public void processNextResponsePackage(final InputStream inputStream) throws PackageEncodeException, IOException {

		final ByteBuffer bb = readNextResponsePackageHeader(inputStream);
		
		if(bb == null) {
			throw new IOException("Read error from socket, exiting");
		}
		
		final ByteBuffer encodedPackage = bboxDBClient.readFullPackage(bb, inputStream);
		bboxDBClient.handleResultPackage(encodedPackage);
	}

	@Override
	protected void beginHook() {
		logger.debug("Started new response reader for " + bboxDBClient.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.debug("Stopping new response reader for " + bboxDBClient.getConnectionName());
	}
	
	@Override
	public void runThread() {
		
		while(bboxDBClient.clientSocket != null) {
			try {
				processNextResponsePackage(bboxDBClient.inputStream);
			} catch(Exception e) {
				// Ignore exceptions when connection is closing
				if(bboxDBClient.getConnectionState().isInRunningState()) {
					bboxDBClient.getConnectionState().dispatchToFailed(e);
				}
			}
		}
		
	}
}

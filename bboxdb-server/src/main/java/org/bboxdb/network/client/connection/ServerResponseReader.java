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
package org.bboxdb.network.client.connection;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.network.packages.PackageEncodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * Read the server response packages
 *
 */
public class ServerResponseReader extends ExceptionSafeRunnable {
	
	/**
	 * The BBOXDB Client
	 */
	protected final BBoxDBConnection bboxDBConnection;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ServerResponseReader.class);
	
	public ServerResponseReader(final BBoxDBConnection bboxDBConnection) {
		this.bboxDBConnection = bboxDBConnection;
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
	 * @throws InterruptedException 
	 */
	public void processNextResponsePackage(final InputStream inputStream) 
			throws PackageEncodeException, IOException, InterruptedException {

		final ByteBuffer bb = readNextResponsePackageHeader(inputStream);
		
		if(bb == null) {
			throw new IOException("Read error from socket, exiting");
		}
		
		final ByteBuffer encodedPackage = bboxDBConnection.readFullPackage(bb, inputStream);
		bboxDBConnection.handleResultPackage(encodedPackage);
	}

	@Override
	protected void beginHook() {
		logger.debug("Started new response reader for {}", bboxDBConnection.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.debug("Stopping new response reader for {}", bboxDBConnection.getConnectionName());
	}
	
	@Override
	public void runThread() {
		
		while(bboxDBConnection.isConnected()) {
			try {
				processNextResponsePackage(bboxDBConnection.getInputStream());
			} catch(Exception e) {
				
				bboxDBConnection.closeSocket();
				
				// Ignore exceptions when connection is closing
				if(bboxDBConnection.getConnectionState().isInRunningState()) {
					bboxDBConnection.getConnectionState().dispatchToFailed(e);
					bboxDBConnection.terminateConnection();
				}
			}
		}
		
	}
}

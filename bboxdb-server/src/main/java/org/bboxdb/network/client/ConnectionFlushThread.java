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

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.network.NetworkConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFlushThread extends ExceptionSafeRunnable {

	/**
	 * The BBOXDB Client
	 */
	private final BBoxDBClient bboxDBClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ConnectionFlushThread.class);
	
	public ConnectionFlushThread(final BBoxDBClient bboxDBClient) {
		this.bboxDBClient = bboxDBClient;
	}

	@Override
	protected void beginHook() {
		logger.debug("Starting connection flush thread for: {}", bboxDBClient.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.debug("Mainteinance flush for: {} has terminated", bboxDBClient.getConnectionName());
	}
	
	@Override
	protected void runThread() throws Exception {
		while(! bboxDBClient.getConnectionState().isInTerminatedState()) {
			// Write all waiting for compression packages
			bboxDBClient.flushPendingCompressionPackages();
			
			try {
				Thread.sleep(NetworkConst.MAX_COMPRESSION_DELAY_MS);
			} catch (InterruptedException e) {
				// Handle InterruptedException directly
				return;
			}
		}
	}

}

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

import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.network.NetworkConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionMainteinanceThread extends ExceptionSafeThread {
	
	/**
	 * The timestamp when the last data was send (useful for sending keep alive packages)
	 */
	protected long lastDataSendTimestamp = 0;
	
	/**
	 * If no data was send for keepAliveTime, a keep alive package is send to the 
	 * server to keep the tcp connection open
	 */
	protected final static long keepAliveTime = TimeUnit.SECONDS.toMillis(30);

	/**
	 * The BBOXDB Client
	 */
	protected final BBoxDBClient bboxDBClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ServerResponseReader.class);
	
	public ConnectionMainteinanceThread(final BBoxDBClient bboxDBClient) {
		this.bboxDBClient = bboxDBClient;
	}

	@Override
	protected void beginHook() {
		logger.debug("Starting connection mainteinance thread for: " + bboxDBClient.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.debug("Mainteinance thread for: " + bboxDBClient.getConnectionName() + " has terminated");
	}
	
	@Override
	public void runThread() {

		while(! bboxDBClient.getConnectionState().isInTerminatedState()) {
			
			// Write all waiting for compression packages
			bboxDBClient.flushPendingCompressionPackages();
						
			if(lastDataSendTimestamp + keepAliveTime < System.currentTimeMillis()) {
				// Send keep alive only on open connections
				if(bboxDBClient.getConnectionState().isInRunningState()) {
					bboxDBClient.sendKeepAlivePackage();
				}
			}
			
			try {
				Thread.sleep(NetworkConst.MAX_COMPRESSION_DELAY_MS);
			} catch (InterruptedException e) {
				// Handle InterruptedException directly
				return;
			}
		}
		
	}
	
	/**
	 * The the timestamp when the last data was send to the server
	 * @return
	 */
	public long getLastDataSendTimestamp() {
		return lastDataSendTimestamp;
	}
	
	/**
	 * Update the last data send timestamp
	 */
	public void updateLastDataSendTimestamp() {
		lastDataSendTimestamp = System.currentTimeMillis();
	}
}
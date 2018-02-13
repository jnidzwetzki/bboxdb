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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.ListHelper;
import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
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
		logger.debug("Starting connection mainteinance thread for: {}", bboxDBClient.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.debug("Mainteinance thread for: {} has terminated", bboxDBClient.getConnectionName());
	}
	
	@Override
	public void runThread() {

		while(! bboxDBClient.getConnectionState().isInTerminatedState()) {
			try {
				// Write all waiting for compression packages
				bboxDBClient.flushPendingCompressionPackages();
							
				if(lastDataSendTimestamp + keepAliveTime < System.currentTimeMillis()) {
					// Send keep alive only on open connections
					if(bboxDBClient.getConnectionState().isInRunningState()) {
						final EmptyResultFuture result = sendKeepAlivePackage();
						waitForResult(result);
					}
				}
			
				Thread.sleep(NetworkConst.MAX_COMPRESSION_DELAY_MS);
			} catch (InterruptedException e) {
				// Handle InterruptedException directly
				return;
			}
		}
	}
	
	/**
	 * Build a keep alive package (with or without gossip)
	 * @return
	 */
	private EmptyResultFuture sendKeepAlivePackage() {
		
		final TupleStoreManagerRegistry tupleStoreManagerRegistry = bboxDBClient.getTupleStoreManagerRegistry();
		if(tupleStoreManagerRegistry == null) {
			return bboxDBClient.sendKeepAlivePackage();
		}
		
		final List<TupleStoreName> tables = tupleStoreManagerRegistry.getAllTables();
		
		if(tables.isEmpty()) {
			return bboxDBClient.sendKeepAlivePackage();
		}
		
		final TupleStoreName tupleStoreName = ListHelper.getElementRandom(tables);
		
		List<ReadOnlyTupleStore> storages = new ArrayList<>();
		try {
			final TupleStoreManager tupleStoreManager = tupleStoreManagerRegistry.getTupleStoreManager(tupleStoreName);

			try {
				storages = tupleStoreManager.aquireStorage();
				
				if(storages.isEmpty()) {
					return bboxDBClient.sendKeepAlivePackage();
				}
				
				final ReadOnlyTupleStore tupleStore = ListHelper.getElementRandom(storages);

				if(tupleStore.getNumberOfTuples() > 0) {
					final Random random = new Random();
					final Tuple tuple = tupleStore.getTupleAtPosition(random.nextInt((int) tupleStore.getNumberOfTuples()));
					
					final String key = tuple.getKey();
					final List<Tuple> tuples = tupleStoreManager.get(key);
					
					logger.debug("Payload in keep alive: {}", tuples);
					
					return bboxDBClient.sendKeepAlivePackage(tupleStoreName.getFullnameWithoutPrefix(), tuples);
				}
			} catch (Exception e) {
				throw e;
			} finally {
				tupleStoreManager.releaseStorage(storages);
			}
		} catch (StorageManagerException e) {
			logger.error("Got exception while reading tuples", e);
		}
		
		return bboxDBClient.sendKeepAlivePackage();
	}

	/**
	 * @param result
	 * @throws InterruptedException
	 */
	private void waitForResult(final EmptyResultFuture result) throws InterruptedException {
		result.waitForAll();
		
		// Gossip has detected an outdated version
		if(result.isFailed()) {
			
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
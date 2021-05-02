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
package org.bboxdb.network.client;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.ListHelper;
import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreAquirer;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionMainteinanceRunnable extends ExceptionSafeRunnable {
	
	/**
	 * The timestamp when the last data was send
	 */
	private long lastDataSendTimestamp = 0;
	
	/**
	 * If no data was send for keepAliveTime, a keep alive package is send to the 
	 * server to keep the tcp connection open
	 */
	private final static long keepAliveTime = TimeUnit.SECONDS.toMillis(30);
	
	/**
	 * The tuple in the last keep alive gossip call
	 */
	private List<Tuple> lastGossipTuples;
	
	/**
	 * The table name of the last gossip tuples
	 */
	private TupleStoreName lastGossipTableName;

	/**
	 * The BBOXDB connection
	 */
	private final BBoxDBConnection connection;
	
	/**
	 * The BBoxDBCliet
	 */
	private final BBoxDBClient bboxDBClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ServerResponseReader.class);
	
	public ConnectionMainteinanceRunnable(final BBoxDBConnection bBoxDBConnection) {
		this.connection = bBoxDBConnection;
		this.bboxDBClient = bBoxDBConnection.getBboxDBClient();
	}

	@Override
	protected void beginHook() {
		logger.debug("Starting connection mainteinance thread for: {}", connection.getConnectionName());
	}
	
	@Override
	protected void endHook() {
		logger.debug("Mainteinance thread for: {} has terminated", connection.getConnectionName());
	}
	
	@Override
	public void runThread() {

		while(! connection.getConnectionState().isInTerminatedState()) {
			try {					
				performDataFlushIfNeeded();
				performKeepAliveIfNeeded();
				Thread.sleep(NetworkConst.MAX_COMPRESSION_DELAY_MS);
			} catch (InterruptedException e) {
				// Handle InterruptedException directly
				return;
			}
		}
	}

	/**
	 * Write all waiting for compression packages
	 */
	private void performDataFlushIfNeeded() {
		final long writenPackages = connection.flushPendingCompressionPackages();
		
		if(writenPackages > 0) {
			updateLastDataSendTimestamp();
		}
	}

	private void performKeepAliveIfNeeded() throws InterruptedException {
		if(lastDataSendTimestamp + keepAliveTime > System.currentTimeMillis()) {
			return;
		}
		
		// Send keep alive only on open connections
		if(connection.getConnectionState().isInRunningState()) {
			final EmptyResultFuture resultFuture = sendKeepAlivePackage();
			waitForResult(resultFuture);
		}
	}
	
	/**
	 * Build a keep alive package (with or without gossip)
	 * @return 
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
		
		lastGossipTableName = ListHelper.getElementRandom(tables);
		
		try {
			final TupleStoreManager tupleStoreManager = tupleStoreManagerRegistry.getTupleStoreManager(lastGossipTableName);

			try(final TupleStoreAquirer tupleStoreAquirer = new TupleStoreAquirer(tupleStoreManager)) {
				final List<ReadOnlyTupleStore> storages = tupleStoreAquirer.getTupleStores();
				
				if(storages.isEmpty()) {
					return bboxDBClient.sendKeepAlivePackage();
				}
				
				final ReadOnlyTupleStore tupleStore = ListHelper.getElementRandom(storages);

				if(tupleStore.getNumberOfTuples() > 0) {
					return sendKeepAliveWithGossip(tupleStoreManager ,tupleStore);
				}
				
			} 
		} catch (StorageManagerException e) {
			logger.error("Got exception while reading tuples", e);
		}
		
		return bboxDBClient.sendKeepAlivePackage();
	}

	/**
	 * Send a keep alive package with some gossip
	 * @param tupleStoreManager
	 * @param tupleStore
	 * @param lastGossipTableName 
	 * @return
	 * @throws StorageManagerException
	 */
	private EmptyResultFuture sendKeepAliveWithGossip(final TupleStoreManager tupleStoreManager,
			final ReadOnlyTupleStore tupleStore) 
			throws StorageManagerException {
		
		final Random random = new Random();
		final Tuple tuple = tupleStore.getTupleAtPosition(random.nextInt((int) tupleStore.getNumberOfTuples()));
		
		final String key = tuple.getKey();
		lastGossipTuples = tupleStoreManager.get(key);
		
		logger.debug("Payload in keep alive: {}", lastGossipTuples);
		
		final String fullnameWithoutPrefix = lastGossipTableName.getFullnameWithoutPrefix();
		return bboxDBClient.sendKeepAlivePackage(fullnameWithoutPrefix, lastGossipTuples);
	}
	
	/**
	 * @param resultFuture 
	 * @param resultFuture
	 * @throws InterruptedException
	 */
	private void waitForResult(final EmptyResultFuture resultFuture) throws InterruptedException {
		
		// Wait for our keep alive to be processed
		resultFuture.waitForCompletion();
		
		// Gossip has detected an outdated version
		if(resultFuture.isFailed()) {
			
			if(lastGossipTuples == null || lastGossipTableName == null) {
				logger.error("Falied keep alive, but no idea what was our last gossip");
				return;
			}
			
			logger.info("Got failed message back from keep alive, "
					+ "outdated tuples detected by gossip {} / {}", lastGossipTableName, lastGossipTuples);
			
			for(final Tuple tuple: lastGossipTuples) {
				try {
					bboxDBClient.insertTuple(lastGossipTableName.getFullnameWithoutPrefix(), tuple);
				} catch (BBoxDBException e) {
					logger.error("Got Exception while performing gossip repair", e);
				}
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
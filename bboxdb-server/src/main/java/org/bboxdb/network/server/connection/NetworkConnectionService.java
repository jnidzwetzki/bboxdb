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
package org.bboxdb.network.server.connection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bboxdb.commons.ServiceState;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkConnectionService implements BBoxDBService {
	
	/**
	 * The configuration
	 */
	private final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();

	/**
	 * Our thread pool to handle connections
	 */
	private ExecutorService threadPool;
	
	/**
	 * The connection handler state
	 */
	private final ServiceState state;
	
	/**
	 * The connection dispatcher runnable
	 */
	private ConnectionDispatcherRunable serverSocketDispatcher;
	
	/**
	 * The thread that listens on the server socket and dispatches
	 * incoming requests to the thread pool
	 */
	private Thread serverSocketDispatchThread;
	
	/**
	 * The storage reference
	 */
	private final TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The lock manager
	 */
	private final LockManager lockManager;
	
	/**
	 * The Logger
	 */
	final static Logger logger = LoggerFactory.getLogger(NetworkConnectionService.class);
	
	public NetworkConnectionService(final TupleStoreManagerRegistry storageRegistry) {
		this.storageRegistry = storageRegistry;
		this.state = new ServiceState();
		this.lockManager = new LockManager();
	}
	
	/**
	 * Start the network connection handler
	 */
	public void init() {
		
		if(! state.isInNewState()) {
			logger.info("init() called on ready instance, ignoring: {}", state.getState());
			return;
		}
		
		try {
			state.dipatchToStarting();
			
			final int port = configuration.getNetworkListenPort();
			logger.info("Start the network connection handler on port: {}", port);
			
			if(threadPool == null) {
				threadPool = Executors.newFixedThreadPool(configuration.getNetworkConnectionThreads());
			}
						
			serverSocketDispatcher = new ConnectionDispatcherRunable(port, threadPool, 
					storageRegistry, lockManager);
			
			serverSocketDispatchThread = new Thread(serverSocketDispatcher);
			serverSocketDispatchThread.start();
			serverSocketDispatchThread.setName("Connection dispatcher thread");
						
			state.dispatchToRunning();
		} catch(Exception e) {
			logger.error("Got exception, setting state to failed", e);
			state.dispatchToFailed(e);
			shutdown();
		}
	}
	
	/**
	 * Shutdown the network connection
	 */
	public synchronized void shutdown() {
		
		if(! state.isInRunningState()) {
			logger.info("shutdown() called non running instance, ignoring: {}", state.getState());
			return;
		}
		
		logger.info("Shutdown the network connection handler");
		state.dispatchToStopping();
		
		if(serverSocketDispatchThread != null) {
			serverSocketDispatcher.closeSocketNE();
			serverSocketDispatchThread.interrupt();	
			serverSocketDispatchThread = null;
			serverSocketDispatcher = null;
		}
		
		if(threadPool != null) {
			threadPool.shutdown();
			threadPool = null;
		}
		
		state.dispatchToTerminated();
	}
	
	@Override
	public String getServicename() {
		return "Network connection handler";
	}
}
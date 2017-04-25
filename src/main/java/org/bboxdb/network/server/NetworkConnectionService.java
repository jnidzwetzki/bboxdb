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
package org.bboxdb.network.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.util.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkConnectionService implements BBoxDBService {
	
	/**
	 * The configuration
	 */
	protected final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();

	/**
	 * Our thread pool to handle connections
	 */
	protected ExecutorService threadPool;
	
	/**
	 * The connection handler state
	 */
	protected final State state = new State(false);
	
	/**
	 * The connection dispatcher runnable
	 */
	protected ConnectionDispatcher serverSocketDispatcher = null;
	
	/**
	 * The thread that listens on the server socket and dispatches
	 * incoming requests to the thread pool
	 */
	protected Thread serverSocketDispatchThread = null;
	
	/**
	 * The state of the server (readonly or readwrite)
	 */
	protected final ServerOperationMode networkConnectionServiceState = new ServerOperationMode();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkConnectionService.class);
	
	/**
	 * Start the network connection handler
	 */
	public void init() {
		
		if(state.isReady()) {
			logger.info("init() called on ready instance, ignoring");
			return;
		}
		
		logger.info("Start the network connection handler on port: " + configuration.getNetworkListenPort());
		
		if(threadPool == null) {
			threadPool = Executors.newFixedThreadPool(configuration.getNetworkConnectionThreads());
		}
		
		networkConnectionServiceState.setReadonly(true);
		
		serverSocketDispatcher = new ConnectionDispatcher();
		serverSocketDispatchThread = new Thread(serverSocketDispatcher);
		serverSocketDispatchThread.start();
		serverSocketDispatchThread.setName("Connection dispatcher thread");
		
		state.setReady(true);
	}
	
	/**
	 * Shutdown our thread pool
	 */
	public void shutdown() {
		
		logger.info("Shutdown the network connection handler");
		
		if(serverSocketDispatchThread != null) {
			serverSocketDispatcher.setShutdown(true);
			serverSocketDispatchThread.interrupt();
			
			serverSocketDispatchThread = null;
			serverSocketDispatcher = null;
		}
		
		if(threadPool != null) {
			threadPool.shutdown();
		}
		
		threadPool = null;
		state.setReady(false);
	}
	
	/**
	 * Is the connection handler ready?
	 * @return
	 */
	public boolean isReady() {
		return state.isReady();
	}
	
	
	/**
	 * The connection dispatcher
	 *
	 */
	class ConnectionDispatcher implements Runnable {

		/**
		 * The shutdown signal
		 */
		protected volatile boolean shutdown = false;
		
		/**
		 * The server socket
		 */
		private ServerSocket serverSocket;

		@Override
		public void run() {
			
			logger.info("Starting new connection dispatcher");
			
			try {
				serverSocket = new ServerSocket(configuration.getNetworkListenPort());
				serverSocket.setReuseAddress(true);
				
				while(! shutdown) {
					final Socket clientSocket = serverSocket.accept();
					handleConnection(clientSocket);
				}
				
			} catch(IOException e) {
				
				// Print exception only if the exception is really unexpected
				if(shutdown != true) {
					logger.error("Got an IO exception while reading from server socket ", e);
					shutdown = true;
				}

			} finally {
				closeSocketNE();
			}
			
			logger.info("Shutting down the connection dispatcher");
		}

		/**
		 * Close socket without an exception
		 */
		protected void closeSocketNE() {
			if(serverSocket != null) {
				try {
					logger.info("Close server socket on port: " + serverSocket.getLocalPort());
					serverSocket.close();
					serverSocket = null;
				} catch (IOException e) {
					// Ignore close exception
				}
			}
		}
		
		/**
		 * Set the shutdown flag
		 * @param shutdown
		 */
		public void setShutdown(final boolean shutdown) {
			this.shutdown = shutdown;
			
			if(shutdown == true) {
				closeSocketNE();
			}
		}
		
		/**
		 * Dispatch the connection to the thread pool
		 * @param clientSocket
		 */
		protected void handleConnection(final Socket clientSocket) {
			logger.debug("Got new connection from: " + clientSocket.getInetAddress());
			threadPool.submit(new ClientConnectionHandler(clientSocket, networkConnectionServiceState));
		}
	}


	@Override
	public String getServicename() {
		return "Network connection handler";
	}
	
	/**
	 * Set the readonly mode
	 * @param readonly
	 */
	public void setReadonly(final boolean readonly) {
		networkConnectionServiceState.setReadonly(readonly);
	}

	/**
	 * Get the readonly mode
	 * @return
	 */
	public boolean isReadonly() {
		return networkConnectionServiceState.isReadonly();
	}
}
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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection dispatcher
 *
 */
public class ConnectionDispatcherRunable extends ExceptionSafeRunnable {

	/**
	 * The server socket
	 */
	private ServerSocket serverSocket;
	
	/**
	 * The listen port
	 */
	private final int port;

	/**
	 * The thread pool for handling connections
	 */
	private final ExecutorService threadPool;

	/**
	 * The storage registry
	 */
	private TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The lock manager
	 */
	private final LockManager lockManager;
	
	/**
	 * The Logger
	 */
	final static Logger logger = LoggerFactory.getLogger(ConnectionDispatcherRunable.class);


	public ConnectionDispatcherRunable(final int port, final ExecutorService threadPool, 
			final TupleStoreManagerRegistry storageRegistry, final LockManager lockManager) {
		
		this.port = port;
		this.threadPool = threadPool;
		this.storageRegistry = storageRegistry;
		this.lockManager = lockManager;
	}

	@Override
	protected void beginHook() {
		logger.info("Starting new connection dispatcher");
	}

	@Override
	protected void endHook() {
		logger.info("Shutting down the connection dispatcher");
	}
	
	@Override
	public void runThread() {			
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);
			
			while(isThreadActive()) {
				final Socket clientSocket = serverSocket.accept();
				handleConnection(clientSocket);
			}
			
		} catch(IOException e) {
			
			// Print exception only if the exception is really unexpected
			if(Thread.currentThread().isInterrupted() != true) {
				logger.error("Got an IO exception while reading from server socket ", e);
			}

		} finally {
			closeSocketNE();
		}
	}

	/**
	 * Is the server socket dispatcher active?
	 * @return
	 */
	private boolean isThreadActive() {
		
		if(Thread.currentThread().isInterrupted()) {
			return false;
		}
		
		if(serverSocket == null) {
			return false;
		}
		
		return true;
	}

	/**
	 * Close socket without an exception
	 */
	public void closeSocketNE() {
		logger.info("Close server socket on port: {}", port);
		CloseableHelper.closeWithoutException(serverSocket);
	}
	
	/**
	 * Dispatch the connection to the thread pool
	 * @param clientSocket
	 */
	private void handleConnection(final Socket clientSocket) {
		logger.debug("Got new connection from: {}", clientSocket.getInetAddress());
		
		final ClientConnectionHandler task = new ClientConnectionHandler(storageRegistry, 
				clientSocket, lockManager);
		
		threadPool.submit(task);
	}
}
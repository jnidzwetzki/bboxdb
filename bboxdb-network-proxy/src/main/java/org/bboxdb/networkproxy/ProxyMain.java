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
package org.bboxdb.networkproxy;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyMain implements Runnable, Closeable {

	/**
	 * The contactpoint
	 */
	private final String contactpoint;
	
	/**
	 * The clustername
	 */
	private final String clustername;
	
	/**
	 * Network port
	 */
	private final int port;
	
	/**
	 * The BBoxDB client
	 */
	private BBoxDB bboxdbClient;
	
	/**
	 * The server socket
	 */
	private ServerSocket serverSocket;
	
	/**
	 * The thread pool for handling connections
	 */
	private final ExecutorService threadPool;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ProxyMain.class);

	public ProxyMain(final String contactpoint, final String clustername) {
		this.contactpoint = contactpoint;
		this.clustername = clustername;
		this.threadPool = Executors.newCachedThreadPool();
		this.port = ProxyConst.PROXY_PORT;
	}

	@Override
	public void run() {
		logger.info("Starting BBoxDB proxy on port: {}", port);
		
		final List<String> connectPoints = Arrays.asList(contactpoint);
		
		// Connect to the BBoxDB cluster
		bboxdbClient = new BBoxDBCluster(connectPoints, clustername);
		bboxdbClient.connect();
		
	    try {
			serverSocket = new ServerSocket(port);
			
		    while(isThreadActive()) {
		    		final Socket clientSocket = serverSocket.accept();
				handleConnection(clientSocket);
		    }
		    
		} catch (IOException e) {
			logger.error("Unable to handle server socket", e);
			System.exit(-1);
		} finally {
			close();
		}
	    
	}
	
	/**
	 * Handle a new client connection
	 * @param clientSocket
	 */
	private void handleConnection(final Socket clientSocket) {
		logger.debug("Handle new connection from: {}", clientSocket.getRemoteSocketAddress());
		final ProxyConnectionRunable proxyConnectionRunable = new ProxyConnectionRunable(clientSocket);
		threadPool.submit(proxyConnectionRunable);
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
	 * Close / Shutdown the instance
	 */
	public void close() {
		logger.info("Shutting down the BBoxDB proxy on port: {}", port);
		
		if(bboxdbClient != null) {
			bboxdbClient.close();
			bboxdbClient = null;
		}
		
		if(serverSocket != null) {
			CloseableHelper.closeWithoutException(serverSocket);
			serverSocket = null;
		}
		
		if(threadPool != null) {
			threadPool.shutdown();
		}
	}
	
	/**
	 * Main * Main * Main * Main * Main * Main * Main
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(final String[] args) throws InterruptedException {
		
		if(args.length != 2) {
			System.err.println("Usage: <Contactpoint> <Clustername>");
			System.exit(-1);
		}
		
		final String contactpoint = args[0];
		final String clustername = args[1];
		
		final ProxyMain main = new ProxyMain(contactpoint, clustername);
		main.run();
		
		while(! Thread.currentThread().isInterrupted()) {
			Thread.sleep(10000);
		}
		
		main.close();
	}
}

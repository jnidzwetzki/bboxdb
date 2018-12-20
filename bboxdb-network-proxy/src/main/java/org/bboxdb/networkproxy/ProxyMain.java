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
import org.bboxdb.commons.service.ServiceState;
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
	private BBoxDBCluster bboxdbClient;

	/**
	 * The server socket
	 */
	private ServerSocket serverSocket;

	/**
	 * The thread pool for handling connections
	 */
	private final ExecutorService threadPool;

	/**
	 * Server thread
	 */
	private Thread serverThread;

	/**
	 * The service state
	 */
	private final ServiceState serviceState;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ProxyMain.class);

	public ProxyMain(final String contactpoint, final String clustername) {
		this.contactpoint = contactpoint;
		this.clustername = clustername;
		this.threadPool = Executors.newCachedThreadPool();
		this.port = ProxyConst.PROXY_PORT;
		this.serviceState = new ServiceState();

		// Close socket on service down
		serviceState.registerCallback((s) -> {
			if(s.isInFinishedState()) {
				logger.info("Executing shutdown callback");

				if(serverThread != null) {
					serverThread.interrupt();
					serverThread = null;
				}

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
		});
	}

	@Override
	public void run() {
		logger.info("Starting BBoxDB proxy on port: {}", port);
		serviceState.reset();
		serviceState.dipatchToStarting();

		final List<String> connectPoints = Arrays.asList(contactpoint);

		// Connect to the BBoxDB cluster
		bboxdbClient = new BBoxDBCluster(connectPoints, clustername);
		bboxdbClient.connect();

		final Runnable run = () -> {
		    try {
				serverSocket = new ServerSocket(port);
				serverSocket.setReuseAddress(true);

				serviceState.dispatchToRunning();

			    while(isThreadActive()) {
			    		final Socket clientSocket = serverSocket.accept();
					handleConnection(clientSocket);
			    }

			} catch (IOException e) {
				if(! Thread.currentThread().isInterrupted()) {
					logger.error("IOException while reading from socket", e);
				} else {
					logger.debug("Exception while reading from interrupted thread socket", e);
				}
			} finally {
				close();
			}
		};

		serverThread = new Thread(run);
		serverThread.start();
	}

	/**
	 * Handle a new client connection
	 * @param clientSocket
	 */
	private void handleConnection(final Socket clientSocket) {
		try {
			logger.debug("Handle new connection from: {}", clientSocket.getRemoteSocketAddress());

			final ProxyConnectionRunable proxyConnectionRunable = new ProxyConnectionRunable(
					bboxdbClient, clientSocket);

			threadPool.submit(proxyConnectionRunable);
		} catch (IOException e) {
			logger.error("Got exception while handling connection", e);
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
	 * Close / Shutdown the instance
	 */
	public void close() {

		if(! serviceState.isInRunningState()) {
			return;
		}

		serviceState.dispatchToStopping();

		serviceState.dispatchToTerminated();
	}

	/**
	 * Get the service state
	 * @return
	 */
	public ServiceState getServiceState() {
		return serviceState;
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

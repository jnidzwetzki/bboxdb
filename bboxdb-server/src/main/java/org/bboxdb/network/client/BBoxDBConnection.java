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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.NetworkInterfaceHelper;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.client.future.HelloFuture;
import org.bboxdb.network.client.future.NetworkOperationFuture;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.response.CompressionHandler;
import org.bboxdb.network.client.response.ErrorHandler;
import org.bboxdb.network.client.response.HelloHandler;
import org.bboxdb.network.client.response.JoinedTupleHandler;
import org.bboxdb.network.client.response.MultipleTupleEndHandler;
import org.bboxdb.network.client.response.MultipleTupleStartHandler;
import org.bboxdb.network.client.response.PageEndHandler;
import org.bboxdb.network.client.response.ServerResponseHandler;
import org.bboxdb.network.client.response.SuccessHandler;
import org.bboxdb.network.client.response.TupleHandler;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.DisconnectRequest;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.PagedTransferableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;

public class BBoxDBConnection {
	
	/**
	 * The sequence number generator
	 */
	private final SequenceNumberGenerator sequenceNumberGenerator;

	/**
	 * The socket of the connection
	 */
	private Socket clientSocket = null;

	/**
	 * The input stream of the socket
	 */
	private BufferedInputStream inputStream;

	/**
	 * The output stream of the socket
	 */
	private BufferedOutputStream outputStream;

	/**
	 * The pending calls
	 */
	private final Map<Short, OperationFuture> pendingCalls = new HashMap<>();

	/**
	 * The result buffer
	 */
	private final Map<Short, List<PagedTransferableEntity>> resultBuffer = new HashMap<>();

	/**
	 * The server response reader
	 */
	private ServerResponseReader serverResponseReader;

	/**
	 * The server response reader thread
	 */
	private Thread serverResponseReaderThread;

	/**
	 * The maintenance handler instance
	 */
	private ConnectionMainteinanceRunnable mainteinanceHandler;

	/**
	 * The maintenance thread
	 */
	private Thread mainteinanceThread;
	
	/**
	 * The maintenance handler instance
	 */
	private ConnectionFlushRunnable flushHandler;

	/**
	 * The maintenance thread
	 */
	private Thread flushThread;

	/**
	 * The default timeout
	 */
	public static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

	/**
	 * The connection state
	 */
	private final ServiceState connectionState;
	
	/**
	 * The maximum amount of in flight requests. Needs to be lower than Short.MAX_VALUE to
	 * prevent two in flight requests with the same id.
	 */
	public final static short MAX_IN_FLIGHT_CALLS = 1000;

	/**
	 * The number of in flight requests
	 * @return
	 */
	private volatile short maxInFlightCalls = MAX_IN_FLIGHT_CALLS;

	/**
	 * The capabilities of the connection
	 */
	private PeerCapabilities connectionCapabilities = new PeerCapabilities();

	/**
	 * The capabilities of the client
	 */
	private PeerCapabilities clientCapabilities = new PeerCapabilities();

	/**
	 * The pending packages for compression
	 */
	private final List<NetworkRequestPackage> pendingCompressionPackages;

	/**
	 * The Server response handler
	 */
	private final Map<Short, ServerResponseHandler> serverResponseHandler;

	/**
	 * The server address
	 */
	private InetSocketAddress serverAddress;

	/**
	 * The BBoxDBClient;
	 */
	private final BBoxDBClient bboxDBClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBClient.class);

	
	@VisibleForTesting
	public BBoxDBConnection() {
		this(new InetSocketAddress("localhost", 1234));
	}

	public BBoxDBConnection(final InetSocketAddress serverAddress) {

		this.serverAddress = Objects.requireNonNull(serverAddress);

		// External IP is used to create proper package routing
		if(serverAddress.getAddress().isLoopbackAddress()) {
			try {
				final Inet4Address nonLoopbackAdress = NetworkInterfaceHelper.getFirstNonLoopbackIPv4();
				this.serverAddress = new InetSocketAddress(nonLoopbackAdress, serverAddress.getPort());
			} catch (SocketException e) {
				logger.error("Connection to loopback IP " + serverAddress 
						+ " requested and unable replace the IP with external IP", e);
			}
		} 

		this.bboxDBClient = new BBoxDBClient(this);
		this.sequenceNumberGenerator = new SequenceNumberGenerator();
		this.connectionState = new ServiceState();

		// Default: Enable gzip compression
		this.clientCapabilities.setGZipCompression();

		this.pendingCompressionPackages = new ArrayList<>();
		this.serverResponseHandler = new HashMap<>();

		initResponseHandler();
	}

	/**
	 * Init the response handler
	 */
	private void initResponseHandler() {
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_COMPRESSION, new CompressionHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_HELLO, new HelloHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_SUCCESS, new SuccessHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_ERROR, new ErrorHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_TUPLE, new TupleHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_START, new MultipleTupleStartHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END, new MultipleTupleEndHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_PAGE_END, new PageEndHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_JOINED_TUPLE, new JoinedTupleHandler());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#connect()
	 */
	public boolean connect() {

		if(clientSocket != null || ! connectionState.isInNewState()) {
			logger.warn("Connect() called on an active connection, ignoring (state: {})", connectionState);
			return true;
		}

		logger.debug("Connecting to server: {}", getConnectionName());

		try {
			connectionState.dipatchToStarting();
			connectionState.registerCallback((c) -> { if(c.isInFailedState() ) { killPendingCalls(); } });
			
			clientSocket = new Socket(serverAddress.getAddress(), serverAddress.getPort());

			inputStream = new BufferedInputStream(clientSocket.getInputStream());
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());

			synchronized (pendingCalls) {
				pendingCalls.clear();
			}

			getResultBuffer().clear();

			// Start up the response reader
			serverResponseReader = new ServerResponseReader(this);
			serverResponseReaderThread = new Thread(serverResponseReader);
			serverResponseReaderThread.setName("Server response reader for " + getConnectionName());
			serverResponseReaderThread.start();

			runHandshake();
		} catch (Exception e) {
			logger.error("Got an exception while connecting to server", e);
			closeSocket();
			connectionState.dispatchToFailed(e);
			return false;
		} 

		return true;
	}

	/**
	 * Close the socket
	 */
	public void closeSocket() {
		logger.info("Closing socket to server: {}", getConnectionName());
		CloseableHelper.closeWithoutException(clientSocket);
		clientSocket = null;
	}

	/**
	 * The name of the connection
	 * @return
	 */
	public String getConnectionName() {
		return serverAddress.getHostString() + " / " + serverAddress.getPort();
	}

	/**
	 * Get the next sequence number
	 * @return
	 */
	public short getNextSequenceNumber() {
		return sequenceNumberGenerator.getNextSequenceNummber();
	}

	/**
	 * Run the handshake with the server
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private void runHandshake() throws Exception {

		if(! connectionState.isInStartingState()) {
			logger.error("Handshaking called in wrong state: {}", connectionState);
		}

		// Capabilities are reported to server; now freeze client capabilities. 
		clientCapabilities.freeze();
	

		NetworkOperationFuture operationFuture = new NetworkOperationFuture(this, () -> {
			return new HelloRequest(getNextSequenceNumber(), 
					NetworkConst.PROTOCOL_VERSION, clientCapabilities);
		});
		
		final HelloFuture helloFuture = new HelloFuture(operationFuture);
		helloFuture.waitForAll();

		if(operationFuture.isFailed()) {
			throw new Exception("Got an error during handshake");
		}

		final HelloResponse helloResponse = helloFuture.get(0);
		connectionCapabilities = helloResponse.getPeerCapabilities();

		connectionState.dispatchToRunning();
		logger.debug("Handshaking with {} done", getConnectionName());
		
		flushHandler = new ConnectionFlushRunnable(this);
		flushThread = new Thread(flushHandler);
		flushThread.setName("Flush thread for: " + getConnectionName());
		flushThread.start();

		mainteinanceHandler = new ConnectionMainteinanceRunnable(this);
		mainteinanceThread = new Thread(mainteinanceHandler);
		mainteinanceThread.setName("Connection mainteinace thread for: " + getConnectionName());
		mainteinanceThread.start();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#disconnect()
	 */
	public void disconnect() {
		
		if(! connectionState.isInRunningState()) {
			logger.error("Unable to disconnect, connection is in state {}", connectionState);
			return;
		}

		synchronized (this) {
			logger.info("Disconnecting from server: {}", getConnectionName());
			connectionState.dispatchToStopping();

			new NetworkOperationFuture(this, () -> {
				return new DisconnectRequest(getNextSequenceNumber());
			});
		}

		settlePendingCalls(DEFAULT_TIMEOUT_MILLIS);

		terminateConnection();
	}

	/**
	 * Settle all pending calls
	 */
	public void settlePendingCalls(final long shutdownTimeMillis) {

		final long shutdownStarted = System.currentTimeMillis();

		// Wait for all pending calls to settle
		synchronized (pendingCalls) {
			
			while(getInFlightCalls() > 0) {
				final long shutdownDuration = System.currentTimeMillis() - shutdownStarted;
				final long timeoutLeft = shutdownTimeMillis - shutdownDuration;

				if(timeoutLeft <= 0) {
					break;
				}
				
				if(! isConnected()) {
					logger.warn("Connection already closed but {} requests are pending", 
							getInFlightCalls());
					return;
				}
				
				logger.info("Waiting up to {} seconds for pending requests to settle "
						+ "(pending {} / server {})", TimeUnit.MILLISECONDS.toSeconds(timeoutLeft), 
						getInFlightCalls(), getConnectionName());
				
				try {
					// Recheck connection state all 5 seconds
					final long maxWaitTime = Math.min(timeoutLeft, TimeUnit.SECONDS.toMillis(5));
					pendingCalls.wait(maxWaitTime);
				} catch (InterruptedException e) {
					logger.debug("Got an InterruptedException during pending calls wait.");
					Thread.currentThread().interrupt();
					return;
				}
			}

			if(! pendingCalls.isEmpty()) {
				logger.warn("Connection is closed. Still pending calls: {} ", pendingCalls);
			}
		}
	}

	/**
	 * Kill all pending requests
	 */
	private void killPendingCalls() {
		synchronized (pendingCalls) {
			if(! pendingCalls.isEmpty()) {
				logger.warn("Socket is closed unexpected, killing pending calls: " + pendingCalls.size());

				for(final short requestId : pendingCalls.keySet()) {
					final OperationFuture future = pendingCalls.get(requestId);
					future.setFailedState();
					future.fireCompleteEvent();
				}

				pendingCalls.clear();
				pendingCalls.notifyAll();
			}
		}
	}

	/**
	 * Close the connection to the server without sending a disconnect package. For a
	 * regular disconnect, see the disconnect() method.
	 */
	public void terminateConnection() {		
		if(connectionState.isInRunningState()) {
			connectionState.dispatchToStopping();
		}

		killPendingCalls();
		getResultBuffer().clear();
		
		closeSocket();

		logger.info("Disconnected from server: {}", getConnectionName());
		connectionState.forceDispatchToTerminated();
		mainteinanceThread.interrupt();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#isConnected()
	 */
	public boolean isConnected() {
		if(clientSocket != null) {
			return ! clientSocket.isClosed();
		}

		return false;
	}

	/**
	 * Get the state of the connection
	 */
	public ServiceState getConnectionState() {
		return connectionState;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#getInFlightCalls()
	 */
	public int getInFlightCalls() {
		synchronized (pendingCalls) {
			return pendingCalls.size();
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#getMaxInFlightCalls()
	 */
	public short getMaxInFlightCalls() {
		return maxInFlightCalls;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#setMaxInFlightCalls(short)
	 */
	public void setMaxInFlightCalls(short maxInFlightCalls) {
		this.maxInFlightCalls = (short) Math.min(maxInFlightCalls, MAX_IN_FLIGHT_CALLS);
	}

	/**
	 * Send a request package to the server
	 * @param responsePackage
	 * @return
	 * @throws IOException 
	 */
	public void sendPackageToServer(final NetworkRequestPackage requestPackage, 
			final NetworkOperationFuture future) {
		
		final short sequenceNumber = requestPackage.getSequenceNumber();
		final boolean result = recalculateRoutingHeader(requestPackage, future);
		
		// Package don't need to be send
		if(result == false) {
			removeFutureAndReleaseSequencenumber(sequenceNumber);
			return;
		}
		
		if(connectionCapabilities.hasGZipCompression()) {
			writePackageWithCompression(requestPackage, future);
		} else {
			writePackageUncompressed(requestPackage, future);
		}
	}

	/**
	 * Recalculate the routing header and handle the exceptions
	 * @param requestPackage
	 * @param future
	 * @return 
	 */
	private boolean recalculateRoutingHeader(final NetworkRequestPackage requestPackage, 
			final NetworkOperationFuture future) {
		
		try {			
			// Check if package needs to be send
			final RoutingHeader routingHeader = requestPackage.getRoutingHeader();
			
			if(routingHeader.isRoutedPackage()) {
				if(routingHeader.getHopCount() == 0) {
					future.setMessage("No distribution regions in next hop, not sending to server");
					future.fireCompleteEvent();
					return false;
				}
			}
			
		} catch (PackageEncodeException e) {
			final String message = "Got a exception during package encoding";
			logger.error(message);
			future.setMessage(message);
			future.setFailedState();
			future.fireCompleteEvent();
			return false;
		}
		
		return true;
	}

	/**
	 * Write a package uncompresssed to the socket
	 * @param requestPackage
	 * @param future
	 */
	private void writePackageUncompressed(final NetworkRequestPackage requestPackage, 
			final NetworkOperationFuture future) {

		try {	
			writePackageToSocket(requestPackage);
		} catch (IOException | PackageEncodeException e) {
			logger.warn("Got an exception while sending package to server", e);
			future.setFailedState();
			future.fireCompleteEvent();
			terminateConnection();
		}
	}

	/**
	 * Handle compression and package chunking
	 * @param requestPackage
	 * @param future
	 */
	private void writePackageWithCompression(NetworkRequestPackage requestPackage, 
			NetworkOperationFuture future) {

		boolean queueFull = false;

		synchronized (pendingCompressionPackages) {
			pendingCompressionPackages.add(requestPackage);
			queueFull = pendingCompressionPackages.size() >= Const.MAX_UNCOMPRESSED_QUEUE_SIZE;
		}

		if(queueFull) {
			flushPendingCompressionPackages();
		}
	}

	/**
	 * Write all pending compression packages to server, called by the maintainance thread
	 * 
	 */
	public void flushPendingCompressionPackages() {

		final List<NetworkRequestPackage> packagesToWrite = new ArrayList<>();

		synchronized (pendingCompressionPackages) {
			if(pendingCompressionPackages.isEmpty()) {
				return;
			}

			packagesToWrite.addAll(pendingCompressionPackages);
			pendingCompressionPackages.clear();
		}

		if(logger.isDebugEnabled()) {
			logger.debug("Chunk size is: {}", packagesToWrite.size());
		}

		final NetworkRequestPackage compressionEnvelopeRequest 
			= new CompressionEnvelopeRequest(NetworkConst.COMPRESSION_TYPE_GZIP, packagesToWrite);

		try {
			writePackageToSocket(compressionEnvelopeRequest);
		} catch (PackageEncodeException | IOException e) {
			logger.error("Got an exception while write pending compression packages to server", e);
			terminateConnection();
		}
	}

	/**
	 * Write the package onto the socket
	 * @param requestPackage
	 * @throws PackageEncodeException
	 * @throws IOException
	 */
	private void writePackageToSocket(final NetworkRequestPackage requestPackage) 
			throws PackageEncodeException, IOException {

		synchronized (outputStream) {
			requestPackage.writeToOutputStream(outputStream);
			outputStream.flush();
		}

		// Could be null during handshake
		if(mainteinanceHandler != null) {
			mainteinanceHandler.updateLastDataSendTimestamp();
		}	
	}

	/**
	 * Register a new package callback
	 * @param requestPackage
	 * @param future
	 * @return
	 */
	public short registerPackageCallback(final NetworkRequestPackage requestPackage, 
			final OperationFuture future) {
		
		final short sequenceNumber = requestPackage.getSequenceNumber();
		future.setRequestId(0, sequenceNumber);

		synchronized (pendingCalls) {
			assert (! pendingCalls.containsKey(sequenceNumber)) 
				: "Old call exists: " + pendingCalls.get(sequenceNumber);
			
			pendingCalls.put(sequenceNumber, future);
		}
		
		try {
			synchronized (pendingCalls) {
				// Ensure that not more then maxInFlightCalls are active
				while(pendingCalls.size() > maxInFlightCalls) {
					pendingCalls.wait();
				}	
			}

		} catch(InterruptedException e) {
			logger.warn("Got an exception while waiting for pending requests", e);
			Thread.currentThread().interrupt();
		}
	
		return sequenceNumber;
	}

	/**
	 * Handle the next result package
	 * @param packageHeader
	 * @throws PackageEncodeException 
	 */
	public void handleResultPackage(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		final short packageType = NetworkPackageDecoder.getPackageTypeFromResponse(encodedPackage);

		OperationFuture future = null;

		synchronized (pendingCalls) {
			future = pendingCalls.get(Short.valueOf(sequenceNumber));
		}

		if(! serverResponseHandler.containsKey(packageType)) {
			logger.error("Unknown respose package type: {}", packageType);
			removeFutureAndReleaseSequencenumber(sequenceNumber);
			
			if(future != null) {
				future.setFailedState();
				future.fireCompleteEvent();
			}

		} else {
			final ServerResponseHandler handler = serverResponseHandler.get(packageType);
			final boolean removeFuture = handler.handleServerResult(this, encodedPackage, future);

			// Remove pending call
			if(removeFuture) {
				removeFutureAndReleaseSequencenumber(sequenceNumber);
			}
		}
	}

	/**
	 * Remove the future from list and release the sequence number
	 * 
	 * @param sequenceNumber
	 */
	private void removeFutureAndReleaseSequencenumber(final short sequenceNumber) {
		synchronized (pendingCalls) {
			sequenceNumberGenerator.releaseNumber(sequenceNumber);
			pendingCalls.remove(Short.valueOf(sequenceNumber));
			pendingCalls.notifyAll();
		}
	}


	/**
	 * Read the full package
	 * @param packageHeader
	 * @param inputStream2 
	 * @return
	 * @throws IOException 
	 */
	public ByteBuffer readFullPackage(final ByteBuffer packageHeader,
			final InputStream inputStream) throws IOException {

		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromResponsePackage(packageHeader);
		final int headerLength = packageHeader.limit();
		final ByteBuffer encodedPackage = ByteBuffer.allocate(headerLength + bodyLength);

		//System.out.println("Trying to read: " + bodyLength + " avail " + inputStream.available());
		encodedPackage.put(packageHeader.array());
		ByteStreams.readFully(inputStream, encodedPackage.array(), encodedPackage.position(), bodyLength);

		return encodedPackage;
	}

	@Override
	public String toString() {
		return "BBoxDBClient [serverHostname=" + serverAddress.getHostString() 
		+ ", serverPort=" + serverAddress.getPort() + ", pendingCalls="
		+ pendingCalls.size() + ", connectionState=" + connectionState + "]";
	}

	/**
	 * Get the capabilities (e.g. gzip compression) of the client
	 */
	public PeerCapabilities getConnectionCapabilities() {
		return connectionCapabilities;
	}

	/**
	 * Get the capabilities of the client
	 * @return
	 */
	public PeerCapabilities getClientCapabilities() {
		return clientCapabilities;
	}

	/**
	 * Get the result buffer
	 * @return
	 */
	public Map<Short, List<PagedTransferableEntity>> getResultBuffer() {
		return resultBuffer;
	}

	/**
	 * Get the server response reader
	 * @return
	 */
	public ServerResponseReader getServerResponseReader() {
		return serverResponseReader;
	}
	
	/**
	 * Get the server address
	 * @return
	 */
	public InetSocketAddress getServerAddress() {
		return serverAddress;
	}
	
	/**
	 * Get the BBoxDB Client
	 * @return
	 */
	public BBoxDBClient getBboxDBClient() {
		return bboxDBClient;
	}
	
	/**
	 * Get the input stream
	 * 
	 * @return
	 */
	public BufferedInputStream getInputStream() {
		return inputStream;
	}
}
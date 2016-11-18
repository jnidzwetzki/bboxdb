/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.network.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkHelper;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.capabilities.PeerCapabilities;
import de.fernunihagen.dna.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;
import de.fernunihagen.dna.scalephant.network.packages.request.CompressionEnvelopeRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.CreateDistributionGroupRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.DeleteDistributionGroupRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.DeleteTableRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.DeleteTupleRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.DisconnectRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.HelloRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.KeepAliveRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.ListTablesRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.QueryBoundingBoxRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.QueryKeyRequest;
import de.fernunihagen.dna.scalephant.network.packages.request.QueryTimeRequest;
import de.fernunihagen.dna.scalephant.network.packages.response.AbstractBodyResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.CompressionEnvelopeResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.ErrorWithBodyResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.HelloResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.ListTablesResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.MultipleTupleEndResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.MultipleTupleStartResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.SuccessWithBodyResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.TupleResponse;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class ScalephantClient implements Scalephant {
	
	/**
	 * The sequence number generator
	 */
	protected final SequenceNumberGenerator sequenceNumberGenerator;
	
	/**
	 * The hostname of the server
	 */
	protected final String serverHostname;
	
	/**
	 * The port of the server
	 */
	protected final int serverPort;
	
	/**
	 * The socket of the connection
	 */
	protected Socket clientSocket = null;

	/**
	 * The input stream of the socket
	 */
	protected BufferedInputStream inputStream;

	/**
	 * The output stream of the socket
	 */
	protected BufferedOutputStream outputStream;
	
	/**
	 * The pending calls
	 */
	protected final Map<Short, ClientOperationFuture> pendingCalls = new HashMap<Short, ClientOperationFuture>();

	/**
	 * The result buffer
	 */
	protected final Map<Short, List<Tuple>> resultBuffer = new HashMap<Short, List<Tuple>>();
	
	/**
	 * The server response reader
	 */
	protected ServerResponseReader serverResponseReader;
	
	/**
	 * The server response reader thread
	 */
	protected Thread serverResponseReaderThread;
	
	/**
	 * The keep alive handler instance
	 */
	protected KeepAliveHandler keepAliveHandler;
	
	/**
	 * The keep alive thread
	 */
	protected Thread keepAliveThread;
	
	/**
	 * The default timeout
	 */
	protected static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

	/**
	 * If no data was send for keepAliveTime, a keep alive package is send to the 
	 * server to keep the tcp connection open
	 */
	protected long keepAliveTime = TimeUnit.SECONDS.toMillis(30);
	
	/**
	 * The connection state
	 */
	protected volatile NetworkConnectionState connectionState;
	
	/**
	 * The number of in flight requests
	 * @return
	 */
	protected volatile short maxInFlightCalls = MAX_IN_FLIGHT_CALLS;

	/**
	 * The capabilities of the connection
	 */
	protected PeerCapabilities connectionCapabilities = new PeerCapabilities();
	
	/**
	 * The capabilities of the client
	 */
	protected PeerCapabilities clientCapabilities = new PeerCapabilities();
	
	/**
	 * The timestamp when the last data was send (useful for sending keep alive packages)
	 */
	protected long lastDataSendTimestamp = 0;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantClient.class);


	public ScalephantClient(final String serverHostname, final int serverPort) {
		super();
		this.serverHostname = serverHostname;
		this.serverPort = serverPort;
		this.sequenceNumberGenerator = new SequenceNumberGenerator();
		this.connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
		
		// Default: Enable gzip compression
		clientCapabilities.setGZipCompression();
	}
	
	public ScalephantClient(final InetSocketAddress address) {
		this(address.getHostString(), address.getPort());
	}

	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#connect()
	 */
	@Override
	public boolean connect() {
		
		if(clientSocket != null) {
			logger.warn("Connect() called on an active connection, ignoring");
			return true;
		}
		
		logger.info("Connecting to server: " + getConnectionName());
		
		try {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING;
			clientSocket = new Socket(serverHostname, serverPort);
			
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
			pendingCalls.clear();
			resultBuffer.clear();
			
			// Start up the response reader
			serverResponseReader = new ServerResponseReader();
			serverResponseReaderThread = new Thread(serverResponseReader);
			serverResponseReaderThread.setName("Server response reader for " + getConnectionName());
			serverResponseReaderThread.start();
			
			runHandshake();
		} catch (Exception e) {
			logger.error("Got an exception while connecting to server", e);
			clientSocket = null;
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			return false;
		} 
		
		return true;
	}
	
	/**
	 * The name of the connection
	 * @return
	 */
	public String getConnectionName() {
		return serverHostname + " / " + serverPort;
	}
	
	/**
	 * Run the handshake with the server
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected void runHandshake() throws Exception {
		
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING) {
			logger.error("Handshaking called in wrong state: " + connectionState);
		}
		
		// Capabilies are reported to server. Make client capabilies read only. 
		clientCapabilities.freeze();
		sendPackageToServer(new HelloRequest(NetworkConst.PROTOCOL_VERSION, clientCapabilities), operationFuture);
		
		operationFuture.waitForAll();
		
		final Object operationResult = operationFuture.get(0);
		if(operationResult instanceof HelloResponse) {
			final HelloResponse heloResponse = (HelloResponse) operationResult;
			connectionCapabilities = heloResponse.getPeerCapabilities();
		} else {
			throw new Exception("Got an error during handshake");
		}
		
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
		logger.info("Handshaking with " + getConnectionName() + " done");
		
		keepAliveHandler = new KeepAliveHandler();
		keepAliveThread = new Thread(keepAliveHandler);
		keepAliveThread.setName("Keep alive thread for: " + serverHostname + " / " + serverPort);
		keepAliveThread.start();
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#disconnect()
	 */
	@Override
	public void disconnect() {
		
		logger.info("Disconnecting from server: " + serverHostname + " port " + serverPort);
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		sendPackageToServer(new DisconnectRequest(), new ClientOperationFuture());

		// Wait for all pending calls to settle
		synchronized (pendingCalls) {
			logger.info("Waiting for pending requests to settle");		
			
			if(! pendingCalls.keySet().isEmpty()) {
				try {
					pendingCalls.wait(DEFAULT_TIMEOUT);
				} catch (InterruptedException e) {
					logger.debug("Got an InterruptedException during pending calls wait.");
					// Close connection immediately
				}
			}
			
			logger.info("All requests are settled. (Non completed requests: " + pendingCalls.size() + ").");
		}
		
		closeConnection();
	}
	
	/**
	 * Kill all pending requests
	 */
	protected void killPendingCalls() {
		synchronized (pendingCalls) {
			if(! pendingCalls.isEmpty()) {
				logger.warn("Socket is closed unexpected, killing pending calls: " + pendingCalls.size());
			
				for(short requestId : pendingCalls.keySet()) {
					final ClientOperationFuture future = pendingCalls.get(requestId);
					future.setFailedState(true);
				}
				
				pendingCalls.clear();
				pendingCalls.notifyAll();
			}
		}
	}

	/**
	 * Close the connection to the server without sending a disconnect package. For a
	 * reagular disconnect, see the disconnect() method.
	 */
	public void closeConnection() {		
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;

		killPendingCalls();
		resultBuffer.clear();
		closeSocketNE();
		
		logger.info("Disconnected from server");
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
	}

	/**
	 * Close socket without any exception
	 */
	protected void closeSocketNE() {
		if(clientSocket != null) {
			try {
				clientSocket.close();
			} catch (IOException e) {
				// Ignore exception on socket close
			}
			clientSocket = null;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#deleteTable(java.lang.String)
	 */
	@Override
	public ClientOperationFuture deleteTable(final String table) {
		
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("deleteTable called, but connection not ready: " + this);
			operationFuture.setFailedState(true);
			return operationFuture;
		}
		
		sendPackageToServer(new DeleteTableRequest(table), operationFuture);
		
		return operationFuture;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#insertTuple(java.lang.String, de.fernunihagen.dna.scalephant.storage.entity.Tuple)
	 */
	@Override
	public ClientOperationFuture insertTuple(final String table, final Tuple tuple) {
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("insertTuple called, but connection not ready: " + this);
			operationFuture.setFailedState(true);
			return operationFuture;
		}
		
		final SSTableName ssTableName = new SSTableName(table);
		
		final InsertTupleRequest requestPackage = new InsertTupleRequest(ssTableName, tuple);
		
		if(connectionCapabilities.hasGZipCompression()) {
			final CompressionEnvelopeRequest compressionEnvelopeRequest = new CompressionEnvelopeRequest(requestPackage, NetworkConst.COMPRESSION_TYPE_GZIP);
			sendPackageToServer(compressionEnvelopeRequest, operationFuture);
		} else {
			sendPackageToServer(requestPackage, operationFuture);
		}
		
		return operationFuture;
	}
	
	public ClientOperationFuture insertTuple(final InsertTupleRequest insertTupleRequest) {
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("insertTuple called, but connection not ready: " + this);
			operationFuture.setFailedState(true);
			return operationFuture;
		}
		
		sendPackageToServer(insertTupleRequest, operationFuture);
		
		return operationFuture;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public ClientOperationFuture deleteTuple(final String table, final String key) {
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("deleteTuple called, but connection not ready: " + this);
			operationFuture.setFailedState(true);
			return operationFuture;
		}
		
		sendPackageToServer(new DeleteTupleRequest(table, key), operationFuture);
		
		return operationFuture;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#listTables()
	 */
	@Override
	public ClientOperationFuture listTables() {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("listTables called, but connection not ready: " + this);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new ListTablesRequest(), future);

		return future;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#createDistributionGroup(java.lang.String, short)
	 */
	@Override
	public ClientOperationFuture createDistributionGroup(final String distributionGroup, final short replicationFactor) {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("listTables called, but connection not ready: " + this);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new CreateDistributionGroupRequest(distributionGroup, replicationFactor), future);

		return future;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#deleteDistributionGroup(java.lang.String)
	 */
	@Override
	public ClientOperationFuture deleteDistributionGroup(final String distributionGroup) {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("listTables called, but connection not ready: " + this);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new DeleteDistributionGroupRequest(distributionGroup), future);

		return future;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#queryKey(java.lang.String, java.lang.String)
	 */
	@Override
	public ClientOperationFuture queryKey(final String table, final String key) {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("queryKey called, but connection not ready: " + this);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new QueryKeyRequest(table, key), future);

		return future;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#queryBoundingBox(java.lang.String, de.fernunihagen.dna.scalephant.storage.entity.BoundingBox)
	 */
	@Override
	public ClientOperationFuture queryBoundingBox(final String table, final BoundingBox boundingBox) {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("queryBoundingBox called, but connection not ready: " + this);
			return null;
		}
		
		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new QueryBoundingBoxRequest(table, boundingBox), future);

		return future;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#queryTime(java.lang.String, long)
	 */
	@Override
	public ClientOperationFuture queryTime(final String table, final long timestamp) {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("queryTime called, but connection not ready: " + this);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new QueryTimeRequest(table, timestamp), future);

		return future;
	}
	
	/**
	 * Send a keep alive package to the server, to keep the TCP connection open.
	 * @return
	 */
	public ClientOperationFuture sendKeepAlivePackage() {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("sendKeepAlivePackage called, but connection not ready: " + this);
			return null;
		}
		
		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new KeepAliveRequest(), future);

		return future;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#isConnected()
	 */
	@Override
	public boolean isConnected() {
		if(clientSocket != null) {
			return ! clientSocket.isClosed();
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#getConnectionState()
	 */
	@Override
	public NetworkConnectionState getConnectionState() {
		return connectionState;
	}

	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#getInFlightCalls()
	 */
	@Override
	public int getInFlightCalls() {
		synchronized (pendingCalls) {
			return pendingCalls.size();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#getMaxInFlightCalls()
	 */
	@Override
	public short getMaxInFlightCalls() {
		return maxInFlightCalls;
	}

	/* (non-Javadoc)
	 * @see de.fernunihagen.dna.scalephant.network.client.Scalephant#setMaxInFlightCalls(short)
	 */
	@Override
	public void setMaxInFlightCalls(short maxInFlightCalls) {
		this.maxInFlightCalls = (short) Math.min(maxInFlightCalls, MAX_IN_FLIGHT_CALLS);
	}

	/**
	 * Send a request package to the server
	 * @param responsePackage
	 * @return
	 * @throws IOException 
	 */
	protected short sendPackageToServer(final NetworkRequestPackage requestPackage, final ClientOperationFuture future) {

		try {
			synchronized (pendingCalls) {
				// Ensure that not more then maxInFlightCalls are active
				while(pendingCalls.size() > maxInFlightCalls) {
					pendingCalls.wait();
				}	
			}
		} catch(InterruptedException e) {
			logger.warn("Got an exception while waiting for pending requests", e);
			return -1;
		}
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		future.setRequestId(0, sequenceNumber);
		
		try {		
			synchronized (pendingCalls) {
				pendingCalls.put(sequenceNumber, future);
			}
			
			synchronized (outputStream) {
				requestPackage.writeToOutputStream(sequenceNumber, outputStream);
				outputStream.flush();
			}
			
			lastDataSendTimestamp = System.currentTimeMillis();
			
		} catch (IOException | PackageEncodeError e) {
			logger.warn("Got an exception while sending package to server", e);
			future.setFailedState(true);
		}
		
		return sequenceNumber;
	}
	
	class KeepAliveHandler implements Runnable {

		@Override
		public void run() {
			logger.debug("Starting keep alive thread for: " + serverHostname + " / " + serverPort);

			while(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				if(lastDataSendTimestamp + keepAliveTime < System.currentTimeMillis()) {
					sendKeepAlivePackage();
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						// Ignore InterruptedException
						break;
					}
				}
			}
			
			logger.debug("Keep alive thread for: " + serverHostname + " / " + serverPort + " has terminated");
		}
	}
	
	/**
	 * Read the server response packages
	 *
	 */
	class ServerResponseReader implements Runnable {
		
		/**
		 * Read the next response package header from the server
		 * @return 
		 * @throws IOException 
		 */
		protected ByteBuffer readNextResponsePackageHeader() throws IOException {
			final ByteBuffer bb = ByteBuffer.allocate(12);
			NetworkHelper.readExactlyBytes(inputStream, bb.array(), 0, bb.limit());
			return bb;
		}
		
		/**
		 * Process the next server answer
		 */
		protected boolean processNextResponsePackage() {
			try {
				final ByteBuffer bb = readNextResponsePackageHeader();
				
				if(bb == null) {
					// Ignore exceptions when connection is closing
					if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
						logger.error("Read error from socket, exiting");
					}
					return false;
				}
				
				final ByteBuffer encodedPackage = readFullPackage(bb);
				handleResultPackage(encodedPackage);
				
			} catch (IOException | PackageEncodeError e) {
				
				// Ignore exceptions when connection is closing
				if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
					logger.error("Unable to read data from server (state: " + connectionState + ")", e);
					return false;
				}
			}
			
			return true;
		}

		/**
		 * Handle the next result package
		 * @param packageHeader
		 * @throws PackageEncodeError 
		 */
		protected void handleResultPackage(final ByteBuffer encodedPackage) throws PackageEncodeError {
			final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
			final short packageType = NetworkPackageDecoder.getPackageTypeFromResponse(encodedPackage);

			ClientOperationFuture pendingCall = null;
			boolean removeFuture = true;
			
			synchronized (pendingCalls) {
				pendingCall = pendingCalls.get(Short.valueOf(sequenceNumber));
			}

			switch(packageType) {
			
				case NetworkConst.RESPONSE_TYPE_COMPRESSION:
					removeFuture = false;
					handleCompression(encodedPackage);
				break;
				
				case NetworkConst.RESPONSE_TYPE_HELLO:
					handleHelo(encodedPackage, pendingCall);
				break;
			
				case NetworkConst.RESPONSE_TYPE_SUCCESS:
					if(pendingCall != null) {
						pendingCall.setOperationResult(0, true);
					}
					break;
					
				case NetworkConst.RESPONSE_TYPE_ERROR:
					if(pendingCall != null) {
						pendingCall.setFailedState(false);
						pendingCall.setOperationResult(0, false);
					}
					break;
					
				case NetworkConst.RESPONSE_TYPE_SUCCESS_WITH_BODY:
					handleSuccessWithBody(encodedPackage, pendingCall);
					break;
					
				case NetworkConst.RESPONSE_TYPE_ERROR_WITH_BODY:
					pendingCall.setFailedState(false);
					handleErrorWithBody(encodedPackage, pendingCall);
					break;
					
				case NetworkConst.RESPONSE_TYPE_LIST_TABLES:
					handleListTables(encodedPackage, pendingCall);
					break;
					
				case NetworkConst.RESPONSE_TYPE_TUPLE:
					// The removal of the future depends, if this is a one
					// tuple result or a multiple tuple result
					removeFuture = handleTuple(encodedPackage, pendingCall);
					break;
					
				case NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_START:
					handleMultiTupleStart(encodedPackage);
					removeFuture = false;
					break;
					
				case NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END:
					handleMultiTupleEnd(encodedPackage, pendingCall);
					break;
					
				default:
					logger.error("Unknown respose package type: " + packageType);
					
					if(pendingCall != null) {
						pendingCall.setFailedState(true);
					}
			}
			
			// Remove pending call
			if(removeFuture) {
				synchronized (pendingCalls) {
					pendingCalls.remove(Short.valueOf(sequenceNumber));
					pendingCalls.notifyAll();
				}
			}
		}

		/**
		 * Kill pending calls
		 */
		protected void handleSocketClosedUnexpected() {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED_WITH_ERRORS;
			killPendingCalls();
		}

		@Override
		public void run() {
			logger.info("Started new response reader for " + serverHostname + " / " + serverPort);
			
			while(clientSocket != null) {
				boolean result = processNextResponsePackage();
				
				if(result == false) {
					handleSocketClosedUnexpected();
					break;
				}
				
			}
			
			logger.info("Stopping new response reader for " + serverHostname + " / " + serverPort);
		}
	}

	/**
	 * Read the full package
	 * @param packageHeader
	 * @return
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader) {
		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromResponsePackage(packageHeader);
		final int headerLength = packageHeader.limit();
		final ByteBuffer encodedPackage = ByteBuffer.allocate(headerLength + bodyLength);
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + inputStream.available());
			encodedPackage.put(packageHeader.array());
			NetworkHelper.readExactlyBytes(inputStream, encodedPackage.array(), encodedPackage.position(), bodyLength);
		} catch (IOException e) {
			logger.error("IO-Exception while reading package", e);
			return null;
		}
		
		return encodedPackage;
	}

	/**
	 * Handle a single tuple as result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeError 
	 */
	protected boolean handleTuple(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) throws PackageEncodeError {
		
		final TupleResponse singleTupleResponse = TupleResponse.decodePackage(encodedPackage);
		final short sequenceNumber = singleTupleResponse.getSequenceNumber();
		
		// Tuple is part of a multi tuple result
		if(resultBuffer.containsKey(sequenceNumber)) {
			resultBuffer.get(sequenceNumber).add(singleTupleResponse.getTuple());
			return false;
		}
		
		// Single tuple is returned
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, singleTupleResponse.getTuple());
		}
		
		return true;
	}

	/**
	 * Handle List table result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeError 
	 */
	protected void handleListTables(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) throws PackageEncodeError {
		final ListTablesResponse tables = ListTablesResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, tables.getTables());
		}
	}

	/**
	 * Handle the compressed package
	 * @param encodedPackage
	 * @throws PackageEncodeError 
	 */
	protected void handleCompression(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final byte[] uncompressedPackage = CompressionEnvelopeResponse.decodePackage(encodedPackage);
		final ByteBuffer uncompressedPackageBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedPackage); 
		serverResponseReader.handleResultPackage(uncompressedPackageBuffer);
	}
	/**
	 * Handle the helo result package
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeError 
	 */
	protected void handleHelo(final ByteBuffer encodedPackage, final ClientOperationFuture pendingCall) throws PackageEncodeError {
		final HelloResponse heloResonse = HelloResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, heloResonse);
		}
	}
	
	/**
	 * Handle error with body result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeError 
	 */
	protected void handleErrorWithBody(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) throws PackageEncodeError {
		final AbstractBodyResponse result = ErrorWithBodyResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, result.getBody());
		}
	}

	/**
	 * Handle success with body result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeError 
	 */
	protected void handleSuccessWithBody(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) throws PackageEncodeError {
		final SuccessWithBodyResponse result = SuccessWithBodyResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, result.getBody());
		}
	}	
	
	/**
	 * Handle the multiple tuple start package
	 * @param encodedPackage
	 * @throws PackageEncodeError 
	 */
	protected void handleMultiTupleStart(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final MultipleTupleStartResponse result = MultipleTupleStartResponse.decodePackage(encodedPackage);
		resultBuffer.put(result.getSequenceNumber(), new ArrayList<Tuple>());
	}
	
	/**
	 * Handle the multiple tuple end package
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeError 
	 */
	protected void handleMultiTupleEnd(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) throws PackageEncodeError {
		final MultipleTupleEndResponse result = MultipleTupleEndResponse.decodePackage(encodedPackage);
		
		final List<Tuple> resultList = resultBuffer.get(result.getSequenceNumber());
		
		if(resultList == null) {
			logger.warn("Got handleMultiTupleEnd and resultList is empty");
			return;
		}
		
		if(pendingCall == null) {
			logger.warn("Got handleMultiTupleEnd and pendingCall is empty");
			return;
		}
		
		pendingCall.setOperationResult(0, resultList);
	}

	@Override
	public String toString() {
		return "ScalephantClient [serverHostname=" + serverHostname + ", serverPort=" + serverPort + ", pendingCalls="
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
	 * The the timestamp when the last data was send to the server
	 * @return
	 */
	public long getLastDataSendTimestamp() {
		return lastDataSendTimestamp;
	}
}

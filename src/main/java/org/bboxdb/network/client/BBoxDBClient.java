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
package org.bboxdb.network.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.HelloFuture;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.SSTableNameListFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CancelQueryRequest;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.DeleteTupleRequest;
import org.bboxdb.network.packages.request.DisconnectRequest;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.ListTablesRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxTimeRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryTimeRequest;
import org.bboxdb.network.packages.response.AbstractBodyResponse;
import org.bboxdb.network.packages.response.CompressionEnvelopeResponse;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.packages.response.ListTablesResponse;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.packages.response.TupleResponse;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.util.MicroSecondTimestampProvider;
import org.bboxdb.util.StreamHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBClient implements BBoxDB {
	
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
	protected final Map<Short, OperationFuture> pendingCalls = new HashMap<Short, OperationFuture>();

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
	 * The maintenance handler instance
	 */
	protected ConnectionMainteinanceThread mainteinanceHandler;
	
	/**
	 * The maintenance thread
	 */
	protected Thread mainteinanceThread;
	
	/**
	 * The default timeout
	 */
	protected static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
	
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
	 * Is the paging for queries enabled?
	 */
	protected boolean pagingEnabled;
	
	/**
	 * The amount of tuples per page
	 */
	protected short tuplesPerPage;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBClient.class);


	public BBoxDBClient(final String serverHostname, final int serverPort) {
		super();
		this.serverHostname = serverHostname;
		this.serverPort = serverPort;
		this.sequenceNumberGenerator = new SequenceNumberGenerator();
		this.connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
		
		// Default: Enable gzip compression
		clientCapabilities.setGZipCompression();
		
		pagingEnabled = true;
		tuplesPerPage = 50;
	}
	
	public BBoxDBClient(final InetSocketAddress address) {
		this(address.getHostString(), address.getPort());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#connect()
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
			
			synchronized (pendingCalls) {
				pendingCalls.clear();
			}
			
			resultBuffer.clear();
			
			// Start up the response reader
			serverResponseReader = new ServerResponseReader(this);
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
	 * Get the next sequence number
	 * @return
	 */
	protected short getNextSequenceNumber() {
		return sequenceNumberGenerator.getNextSequenceNummber();
	}
	
	/**
	 * Run the handshake with the server
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected void runHandshake() throws Exception {
		
		final HelloFuture operationFuture = new HelloFuture(1);

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING) {
			logger.error("Handshaking called in wrong state: " + connectionState);
		}
		
		// Capabilies are reported to server. Make client capabilies read only. 
		clientCapabilities.freeze();
		final HelloRequest requestPackage = new HelloRequest(getNextSequenceNumber(), 
				NetworkConst.PROTOCOL_VERSION, clientCapabilities);
		
		registerPackageCallback(requestPackage, operationFuture);
		sendPackageToServer(requestPackage, operationFuture);
		
		operationFuture.waitForAll();
		
		if(operationFuture.isFailed()) {
			throw new Exception("Got an error during handshake");
		}
		
		final HelloResponse helloResponse = operationFuture.get(0);
		connectionCapabilities = helloResponse.getPeerCapabilities();

		connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
		logger.info("Handshaking with " + getConnectionName() + " done");
		
		mainteinanceHandler = new ConnectionMainteinanceThread(this);
		mainteinanceThread = new Thread(mainteinanceHandler);
		mainteinanceThread.setName("Connection mainteinace thread for: " + getConnectionName());
		mainteinanceThread.start();
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#disconnect()
	 */
	@Override
	public void disconnect() {
		
		logger.info("Disconnecting from server: " + serverHostname + " port " + serverPort);
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		
		final DisconnectRequest requestPackage = new DisconnectRequest(getNextSequenceNumber());
		final EmptyResultFuture operationFuture = new EmptyResultFuture(1);
		
		registerPackageCallback(requestPackage, operationFuture);
		sendPackageToServer(requestPackage, operationFuture);

		// Wait for all pending calls to settle
		synchronized (pendingCalls) {
			logger.info("Waiting {} seconds for pending requests to settle", DEFAULT_TIMEOUT / 1000);		
			
			if(! pendingCalls.keySet().isEmpty()) {
				try {
					pendingCalls.wait(DEFAULT_TIMEOUT);
				} catch (InterruptedException e) {
					logger.debug("Got an InterruptedException during pending calls wait.");
					Thread.currentThread().interrupt();
					// Close connection immediately
				}
			}
			
			logger.info("Connection is closed. (Non completed requests: " + pendingCalls.size() + ").");
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
	
	/**
	 * Create a failed SSTableNameListFuture
	 * @return
	 */
	protected SSTableNameListFuture createFailedSStableNameFuture(final String errorMessage) {
		final SSTableNameListFuture clientOperationFuture = new SSTableNameListFuture(1);
		clientOperationFuture.setMessage(0, errorMessage);
		clientOperationFuture.setFailedState();
		clientOperationFuture.fireCompleteEvent(); 
		return clientOperationFuture;
	}
	
	/**
	 * Create a failed tuple list future
	 * @return
	 */
	protected TupleListFuture createFailedTupleListFuture(final String errorMessage) {
		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		clientOperationFuture.setMessage(0, errorMessage);
		clientOperationFuture.setFailedState();
		clientOperationFuture.fireCompleteEvent(); 
		return clientOperationFuture;
	}
	
	/**
	 * Create a new failed future object
	 * @return
	 */
	protected EmptyResultFuture createFailedFuture(final String errorMessage) {
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		clientOperationFuture.setMessage(0, errorMessage);
		clientOperationFuture.setFailedState();
		clientOperationFuture.fireCompleteEvent();
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#deleteTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTable(final String table) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("deleteTable called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final DeleteTableRequest requestPackage = new DeleteTableRequest(getNextSequenceNumber(), table);
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("insertTuple called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final SSTableName ssTableName = new SSTableName(table);
		final short sequenceNumber = getNextSequenceNumber();
		
		final InsertTupleRequest requestPackage = new InsertTupleRequest(sequenceNumber, ssTableName, tuple);
		
		if(connectionCapabilities.hasGZipCompression()) {
			final List<NetworkRequestPackage> requestPackages = new ArrayList<>();
			requestPackages.add(requestPackage);
			
			final CompressionEnvelopeRequest compressionEnvelopeRequest 
				= new CompressionEnvelopeRequest(NetworkConst.COMPRESSION_TYPE_GZIP, 
						requestPackages);
			
			for(final NetworkRequestPackage myPackage: requestPackages) {
				registerPackageCallback(myPackage, clientOperationFuture);
			}
			
			sendPackageToServer(compressionEnvelopeRequest, clientOperationFuture);
			
		} else {
			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);
		}
		
		return clientOperationFuture;
	}
	
	
	public EmptyResultFuture insertTuple(final InsertTupleRequest insertTupleRequest) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("insertTuple called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		sendPackageToServer(insertTupleRequest, clientOperationFuture);
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) {

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("deleteTuple called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final DeleteTupleRequest requestPackage = new DeleteTupleRequest(getNextSequenceNumber(), 
				table, key, timestamp);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key) {
		final long timestamp = MicroSecondTimestampProvider.getNewTimestamp();
		return deleteTuple(table, key, timestamp);
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#listTables()
	 */
	@Override
	public SSTableNameListFuture listTables() {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedSStableNameFuture("listTables called, but connection not ready: " + this);
		}

		final SSTableNameListFuture clientOperationFuture = new SSTableNameListFuture(1);
		final ListTablesRequest requestPackage = new ListTablesRequest(getNextSequenceNumber());
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#createDistributionGroup(java.lang.String, short)
	 */
	@Override
	public EmptyResultFuture createDistributionGroup(final String distributionGroup, 
			final short replicationFactor) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("listTables called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final CreateDistributionGroupRequest requestPackage = new CreateDistributionGroupRequest(
				getNextSequenceNumber(), distributionGroup, replicationFactor);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#deleteDistributionGroup(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("delete distribution group called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final DeleteDistributionGroupRequest requestPackage = new DeleteDistributionGroupRequest(
				getNextSequenceNumber(), distributionGroup);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#queryKey(java.lang.String, java.lang.String)
	 */
	@Override
	public TupleListFuture queryKey(final String table, final String key) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedTupleListFuture("queryKey called, but connection not ready: " + this);
		}

		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final QueryKeyRequest requestPackage = new QueryKeyRequest(getNextSequenceNumber(), table, key);
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#queryBoundingBox(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryBoundingBox(final String table, final BoundingBox boundingBox) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedTupleListFuture("queryBoundingBox called, but connection not ready: " + this);
		}
		
		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final QueryBoundingBoxRequest requestPackage = new QueryBoundingBoxRequest(getNextSequenceNumber(), 
				table, boundingBox, pagingEnabled, tuplesPerPage);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#queryBoundingBoxAndTime(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryBoundingBoxAndTime(final String table,
			final BoundingBox boundingBox, final long timestamp) {

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedTupleListFuture("queryBoundingBox called, but connection not ready: " + this);
		}
		
		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final QueryBoundingBoxTimeRequest requestPackage = new QueryBoundingBoxTimeRequest(getNextSequenceNumber(), 
				table, boundingBox, timestamp, pagingEnabled, tuplesPerPage);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryTime(final String table, final long timestamp) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final QueryTimeRequest requestPackage = new QueryTimeRequest(getNextSequenceNumber(), 
				table, timestamp, pagingEnabled, tuplesPerPage);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/**
	 * Send a keep alive package to the server, to keep the TCP connection open.
	 * @return
	 */
	public EmptyResultFuture sendKeepAlivePackage() {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedFuture("sendKeepAlivePackage called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final KeepAliveRequest requestPackage = new KeepAliveRequest(getNextSequenceNumber());
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);

		return clientOperationFuture;
	}
	
	/**
	 * Get the next page for a given query
	 * @param queryPackageId
	 * @return
	 */
	public OperationFuture getNextPage(final short queryPackageId) {
		
		if(! resultBuffer.containsKey(queryPackageId)) {
			final String errorMessage = "Query package " + queryPackageId 
					+ " not found in the result buffer";
			
			logger.error(errorMessage);
			return createFailedTupleListFuture(errorMessage);
		}
		
		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final NextPageRequest requestPackage = new NextPageRequest(
				getNextSequenceNumber(), queryPackageId);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/**
	 * Cancel the given query on the server
	 * @param queryPackageId
	 * @return
	 */
	public EmptyResultFuture cancelQuery(final short queryPackageId) {
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		
		final CancelQueryRequest requestPackage = new CancelQueryRequest(getNextSequenceNumber(), queryPackageId);
		
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#isConnected()
	 */
	@Override
	public boolean isConnected() {
		if(clientSocket != null) {
			return ! clientSocket.isClosed();
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#getConnectionState()
	 */
	@Override
	public NetworkConnectionState getConnectionState() {
		return connectionState;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#getInFlightCalls()
	 */
	@Override
	public int getInFlightCalls() {
		synchronized (pendingCalls) {
			return pendingCalls.size();
		}
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#getMaxInFlightCalls()
	 */
	@Override
	public short getMaxInFlightCalls() {
		return maxInFlightCalls;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#setMaxInFlightCalls(short)
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
	protected void sendPackageToServer(final NetworkRequestPackage requestPackage, 
			final OperationFuture future) {

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
			return;
		}
				
		try {		
			synchronized (outputStream) {
				requestPackage.writeToOutputStream(outputStream);
				outputStream.flush();
			}
			
			// Could be null during handshake
			if(mainteinanceHandler != null) {
				mainteinanceHandler.updateLastDataSendTimestamp();
			}
		} catch (IOException | PackageEncodeException e) {
			logger.warn("Got an exception while sending package to server", e);
			future.setFailedState();
			future.fireCompleteEvent();
		}
		
	}

	protected short registerPackageCallback(final NetworkRequestPackage requestPackage, final OperationFuture future) {
		final short sequenceNumber = requestPackage.getSequenceNumber();
		future.setRequestId(0, sequenceNumber);
		
		synchronized (pendingCalls) {
			assert (! pendingCalls.containsKey(sequenceNumber)) 
				: "Old call exists: " + pendingCalls.get(sequenceNumber);
			
			pendingCalls.put(sequenceNumber, future);
		}
		return sequenceNumber;
	}
	
	/**
	 * Handle the next result package
	 * @param packageHeader
	 * @throws PackageEncodeException 
	 */
	protected void handleResultPackage(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		final short packageType = NetworkPackageDecoder.getPackageTypeFromResponse(encodedPackage);

		OperationFuture pendingCall = null;
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
				handleHello(encodedPackage, (HelloFuture) pendingCall);
			break;
				
			case NetworkConst.RESPONSE_TYPE_SUCCESS:
				handleSuccess(encodedPackage, pendingCall);
				break;
				
			case NetworkConst.RESPONSE_TYPE_ERROR:
				handleError(encodedPackage, pendingCall);
				break;
				
			case NetworkConst.RESPONSE_TYPE_LIST_TABLES:
				handleListTables(encodedPackage, (SSTableNameListFuture) pendingCall);
				break;
				
			case NetworkConst.RESPONSE_TYPE_TUPLE:
				// The removal of the future depends, if this is a one
				// tuple result or a multiple tuple result
				removeFuture = handleTuple(encodedPackage, (TupleListFuture) pendingCall);
				break;
				
			case NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_START:
				handleMultiTupleStart(encodedPackage);
				removeFuture = false;
				break;
				
			case NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END:
				handleMultiTupleEnd(encodedPackage, (TupleListFuture) pendingCall);
				break;
				
			case NetworkConst.RESPONSE_TYPE_PAGE_END:
				handlePageEnd(encodedPackage, (TupleListFuture) pendingCall);
				break;
				
			default:
				logger.error("Unknown respose package type: " + packageType);
				
				if(pendingCall != null) {
					pendingCall.setFailedState();
					pendingCall.fireCompleteEvent();
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
	 * Read the full package
	 * @param packageHeader
	 * @param inputStream2 
	 * @return
	 * @throws IOException 
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader,
			final InputStream inputStream) throws IOException {
		
		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromResponsePackage(packageHeader);
		final int headerLength = packageHeader.limit();
		final ByteBuffer encodedPackage = ByteBuffer.allocate(headerLength + bodyLength);
		
		//System.out.println("Trying to read: " + bodyLength + " avail " + inputStream.available());
		encodedPackage.put(packageHeader.array());
		StreamHelper.readExactlyBytes(inputStream, encodedPackage.array(), encodedPackage.position(), bodyLength);

		return encodedPackage;
	}

	/**
	 * Handle a single tuple as result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException 
	 */
	protected boolean handleTuple(final ByteBuffer encodedPackage,
			final TupleListFuture pendingCall) throws PackageEncodeException {
		
		final TupleResponse singleTupleResponse = TupleResponse.decodePackage(encodedPackage);
		final short sequenceNumber = singleTupleResponse.getSequenceNumber();
		
		// Tuple is part of a multi tuple result
		if(resultBuffer.containsKey(sequenceNumber)) {
			resultBuffer.get(sequenceNumber).add(singleTupleResponse.getTuple());
			return false;
		}
		
		// Single tuple is returned
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, Arrays.asList(singleTupleResponse.getTuple()));
			pendingCall.fireCompleteEvent();
		}
		
		return true;
	}

	/**
	 * Handle List table result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException 
	 */
	protected void handleListTables(final ByteBuffer encodedPackage,
			final SSTableNameListFuture pendingCall) throws PackageEncodeException {
		final ListTablesResponse tables = ListTablesResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, tables.getTables());
			pendingCall.fireCompleteEvent();
		}
	}

	/**
	 * Handle the compressed package
	 * @param encodedPackage
	 * @throws PackageEncodeException 
	 */
	protected void handleCompression(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final InputStream uncompressedStream = CompressionEnvelopeResponse.decodePackage(encodedPackage);
		
		try {
			while(uncompressedStream.available() > 0) {
				serverResponseReader.processNextResponsePackage(uncompressedStream);
			}
		} catch (IOException e) {
			logger.error("Got IO error while handling compressed packages", e);
		}
		
	}
	
	/**
	 * Handle the helo result package
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException 
	 */
	protected void handleHello(final ByteBuffer encodedPackage, final HelloFuture pendingCall) throws PackageEncodeException {
		final HelloResponse helloResponse = HelloResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setOperationResult(0, helloResponse);
			pendingCall.fireCompleteEvent();
		}
	}
	
	/**
	 * Handle error with body result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException 
	 */
	protected void handleError(final ByteBuffer encodedPackage,
			final OperationFuture pendingCall) throws PackageEncodeException {
		
		final AbstractBodyResponse result = ErrorResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setMessage(0, result.getBody());
			pendingCall.setFailedState();
			pendingCall.fireCompleteEvent();
		}
	}

	/**
	 * Handle success with body result
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException 
	 */
	protected void handleSuccess(final ByteBuffer encodedPackage,
			final OperationFuture pendingCall) throws PackageEncodeException {
		
		final AbstractBodyResponse result = SuccessResponse.decodePackage(encodedPackage);
		
		if(pendingCall != null) {
			pendingCall.setMessage(0, result.getBody());
			pendingCall.fireCompleteEvent();
		}
	}	
	
	/**
	 * Handle the multiple tuple start package
	 * @param encodedPackage
	 * @throws PackageEncodeException 
	 */
	protected void handleMultiTupleStart(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final MultipleTupleStartResponse result = MultipleTupleStartResponse.decodePackage(encodedPackage);
		resultBuffer.put(result.getSequenceNumber(), new ArrayList<Tuple>());
	}
	
	/**
	 * Handle the multiple tuple end package
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException 
	 */
	protected void handleMultiTupleEnd(final ByteBuffer encodedPackage,
			final TupleListFuture pendingCall) throws PackageEncodeException {
		
		final MultipleTupleEndResponse result = MultipleTupleEndResponse.decodePackage(encodedPackage);
		
		final short sequenceNumber = result.getSequenceNumber();
		final List<Tuple> resultList = resultBuffer.remove(sequenceNumber);

		if(pendingCall == null) {
			logger.warn("Got handleMultiTupleEnd and pendingCall is empty (package {}) ",
					sequenceNumber);
			return;
		}
		
		if(resultList == null) {
			logger.warn("Got handleMultiTupleEnd and resultList is empty (package {})",
					sequenceNumber);
			
			pendingCall.setFailedState();
			pendingCall.fireCompleteEvent();
			return;
		}
		
		pendingCall.setCompleteResult(0, true);
		pendingCall.setOperationResult(0, resultList);
		pendingCall.fireCompleteEvent();
	}
	
	/**
	 * Handle the end of a page
	 * @param encodedPackage
	 * @param pendingCall
	 * @throws PackageEncodeException
	 */
	protected void handlePageEnd(final ByteBuffer encodedPackage,
			final TupleListFuture pendingCall) throws PackageEncodeException {
		
		if(pendingCall == null) {
			logger.warn("Got handleMultiTupleEnd and pendingCall is empty");
			return;
		}

		final PageEndResponse result = PageEndResponse.decodePackage(encodedPackage);
		final short sequenceNumber = result.getSequenceNumber();

		final List<Tuple> resultList = resultBuffer.remove(sequenceNumber);
		
		// Collect tuples of the next page in new list
		resultBuffer.put(sequenceNumber, new ArrayList<Tuple>());
		
		if(resultList == null) {
			logger.warn("Got handleMultiTupleEnd and resultList is empty");
			pendingCall.setFailedState();
			pendingCall.fireCompleteEvent();
			return;
		}

		pendingCall.setConnectionForResult(0, this);
		pendingCall.setCompleteResult(0, false);
		pendingCall.setOperationResult(0, resultList);
		pendingCall.fireCompleteEvent();
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
	 * Is the paging for queries enables
	 * @return
	 */
	public boolean isPagingEnabled() {
		return pagingEnabled;
	}

	/**
	 * Enable or disable paging
	 * @param pagingEnabled
	 */
	public void setPagingEnabled(final boolean pagingEnabled) {
		this.pagingEnabled = pagingEnabled;
	}

	/**
	 * Get the amount of tuples per page
	 * @return
	 */
	public short getTuplesPerPage() {
		return tuplesPerPage;
	}

	/**
	 * Set the tuples per page
	 * @param tuplesPerPage
	 */
	public void setTuplesPerPage(final short tuplesPerPage) {
		this.tuplesPerPage = tuplesPerPage;
	}
}

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
import org.bboxdb.network.client.response.CompressionHandler;
import org.bboxdb.network.client.response.ErrorHandler;
import org.bboxdb.network.client.response.HelloHandler;
import org.bboxdb.network.client.response.ListTablesHandler;
import org.bboxdb.network.client.response.MultipleTupleEndHandler;
import org.bboxdb.network.client.response.MultipleTupleStartHandler;
import org.bboxdb.network.client.response.PageEndHandler;
import org.bboxdb.network.client.response.ServerResponseHandler;
import org.bboxdb.network.client.response.SuccessHandler;
import org.bboxdb.network.client.response.TupleHandler;
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
import org.bboxdb.network.packages.request.QueryInsertTimeRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryVersionTimeRequest;
import org.bboxdb.network.packages.response.HelloResponse;
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
	private final Map<Short, List<Tuple>> resultBuffer = new HashMap<Short, List<Tuple>>();
	
	/**
	 * The server response reader
	 */
	private ServerResponseReader serverResponseReader;
	
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
	 * The pending packages for compression
	 */
	protected final List<NetworkRequestPackage> pendingCompressionPackages;

	/**
	 * The Server response handler
	 */
	protected final Map<Short, ServerResponseHandler> serverResponseHandler;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBClient.class);

	public BBoxDBClient(final String serverHostname, final int serverPort) {
		this.serverHostname = serverHostname;
		this.serverPort = serverPort;
		this.sequenceNumberGenerator = new SequenceNumberGenerator();
		this.connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
		
		// Default: Enable gzip compression
		clientCapabilities.setGZipCompression();
		
		pagingEnabled = true;
		tuplesPerPage = 50;
		pendingCompressionPackages = new ArrayList<>();
		serverResponseHandler = new HashMap<>();
		initResponseHandler();
	}
	
	public BBoxDBClient(final InetSocketAddress address) {
		this(address.getHostString(), address.getPort());
	}

	protected void initResponseHandler() {
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_COMPRESSION, new CompressionHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_HELLO, new HelloHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_SUCCESS, new SuccessHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_ERROR, new ErrorHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_LIST_TABLES, new ListTablesHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_TUPLE, new TupleHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_START, new MultipleTupleStartHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END, new MultipleTupleEndHandler());
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_PAGE_END, new PageEndHandler());
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
		
		logger.debug("Connecting to server: {}", getConnectionName());
		
		try {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING;
			clientSocket = new Socket(serverHostname, serverPort);
			
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
		
		// Capabilities are reported to server; now freeze client capabilities. 
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
		logger.debug("Handshaking with {} done", getConnectionName());
		
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
		
		synchronized (this) {
			logger.info("Disconnecting from server: {}", getConnectionName());
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
			
			final DisconnectRequest requestPackage = new DisconnectRequest(getNextSequenceNumber());
			final EmptyResultFuture operationFuture = new EmptyResultFuture(1);
			
			registerPackageCallback(requestPackage, operationFuture);
			sendPackageToServer(requestPackage, operationFuture);
		}

		// Wait for all pending calls to settle
		synchronized (pendingCalls) {
			
			if(! pendingCalls.keySet().isEmpty()) {
				
				logger.info("Waiting {} seconds for pending requests to settle", 
						TimeUnit.MILLISECONDS.toSeconds(DEFAULT_TIMEOUT));		
				
				try {
					pendingCalls.wait(DEFAULT_TIMEOUT);
				} catch (InterruptedException e) {
					logger.debug("Got an InterruptedException during pending calls wait.");
					Thread.currentThread().interrupt();
					// Close connection immediately
				}
			}
			
			if(! pendingCalls.isEmpty()) {
				logger.warn("Connection is closed. Still pending calls: {} ", pendingCalls);
			}
		}
		
		terminateConnection();
	}
	
	/**
	 * Kill all pending requests
	 */
	protected void killPendingCalls() {
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
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;

		killPendingCalls();
		getResultBuffer().clear();
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
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);

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
	public TupleListFuture queryVersionTime(final String table, final long timestamp) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final QueryVersionTimeRequest requestPackage = new QueryVersionTimeRequest(getNextSequenceNumber(), 
				table, timestamp, pagingEnabled, tuplesPerPage);
		
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		
		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.Scalephant#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) {
		
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			return createFailedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		final TupleListFuture clientOperationFuture = new TupleListFuture(1);
		final QueryInsertTimeRequest requestPackage = new QueryInsertTimeRequest(getNextSequenceNumber(), 
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
		
		synchronized (this) {
			if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				return createFailedFuture("sendKeepAlivePackage called, but connection not ready: " + this);
			}
			
			final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
			final KeepAliveRequest requestPackage = new KeepAliveRequest(getNextSequenceNumber());
			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);
			return clientOperationFuture;
		}
		
	}
	
	/**
	 * Get the next page for a given query
	 * @param queryPackageId
	 * @return
	 */
	public OperationFuture getNextPage(final short queryPackageId) {
		
		if(! getResultBuffer().containsKey(queryPackageId)) {
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
		
		future.setConnectionName(0, getConnectionName());

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
		
		if(connectionCapabilities.hasGZipCompression()) {
			writePackageWithCompression(requestPackage, future);
		} else {
			writePackageUncompressed(requestPackage, future);
		}
	}

	/**
	 * Write a package uncompresssed to the socket
	 * @param requestPackage
	 * @param future
	 */
	protected void writePackageUncompressed(final NetworkRequestPackage requestPackage, 
			final OperationFuture future) {
		
		try {	
			writePackageToSocket(requestPackage);
		} catch (IOException | PackageEncodeException e) {
			logger.warn("Got an exception while sending package to server", e);
			future.setFailedState();
			future.fireCompleteEvent();
		}
	}

	/**
	 * Handle compression and package chunking
	 * @param requestPackage
	 * @param future
	 */
	protected void writePackageWithCompression(NetworkRequestPackage requestPackage, OperationFuture future) {
		synchronized (pendingCompressionPackages) {
			pendingCompressionPackages.add(requestPackage);
		}
	}
	
	/**
	 * Write all pending compression packages to server, called by the maintainance thread
	 * 
	 */
	protected void flushPendingCompressionPackages() {
		
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
		}
	}
	
	/**
	 * Write the package onto the socket
	 * @param requestPackage
	 * @throws PackageEncodeException
	 * @throws IOException
	 */
	protected void writePackageToSocket(final NetworkRequestPackage requestPackage) 
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
	
		OperationFuture future = null;
		
		synchronized (pendingCalls) {
			future = pendingCalls.get(Short.valueOf(sequenceNumber));
		}
		
		if(! serverResponseHandler.containsKey(packageType)) {
			logger.error("Unknown respose package type: " + packageType);
			
			if(future != null) {
				future.setFailedState();
				future.fireCompleteEvent();
			}
			
		} else {
			
			final ServerResponseHandler handler = serverResponseHandler.get(packageType);
			final boolean removeFuture = handler.handleServerResult(this, encodedPackage, future);
			
			// Remove pending call
			if(removeFuture) {
				synchronized (pendingCalls) {
					pendingCalls.remove(Short.valueOf(sequenceNumber));
					pendingCalls.notifyAll();
				}
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
	 * Process the next response package
	 * @param inputStream
	 * @return
	 */
	public boolean processNextResponsePackage(final InputStream inputStream) {
		return serverResponseReader.processNextResponsePackage(inputStream);
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

	public Map<Short, List<Tuple>> getResultBuffer() {
		return resultBuffer;
	}
}

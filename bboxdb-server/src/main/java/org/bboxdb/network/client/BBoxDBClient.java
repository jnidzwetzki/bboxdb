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
import java.util.stream.Collectors;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.NetworkInterfaceHelper;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.FutureHelper;
import org.bboxdb.network.client.future.HelloFuture;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.SSTableNameListFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.client.response.CompressionHandler;
import org.bboxdb.network.client.response.ErrorHandler;
import org.bboxdb.network.client.response.HelloHandler;
import org.bboxdb.network.client.response.JoinedTupleHandler;
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
import org.bboxdb.network.packages.request.CreateTableRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.DeleteTupleRequest;
import org.bboxdb.network.packages.request.DisconnectRequest;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.ListTablesRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxContinuousRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxTimeRequest;
import org.bboxdb.network.packages.request.QueryInsertTimeRequest;
import org.bboxdb.network.packages.request.QueryJoinRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryVersionTimeRequest;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.routing.RoutingHopHelper;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.PagedTransferableEntity;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;

public class BBoxDBClient implements BBoxDB {

	/**
	 * The sequence number generator
	 */
	protected final SequenceNumberGenerator sequenceNumberGenerator;

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
	protected final Map<Short, OperationFuture> pendingCalls = new HashMap<>();

	/**
	 * The result buffer
	 */
	protected final Map<Short, List<PagedTransferableEntity>> resultBuffer = new HashMap<>();

	/**
	 * The retryer
	 */
	protected final NetworkOperationRetryer networkOperationRetryer;

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
	public static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

	/**
	 * The connection state
	 */
	protected final ServiceState connectionState;

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
	 * The server address
	 */
	protected InetSocketAddress serverAddress;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBClient.class);
	
	@VisibleForTesting
	public BBoxDBClient() {
		this(new InetSocketAddress("localhost", 1234));
	}

	public BBoxDBClient(final InetSocketAddress serverAddress) {

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

		this.sequenceNumberGenerator = new SequenceNumberGenerator();
		this.connectionState = new ServiceState();

		// Default: Enable gzip compression
		clientCapabilities.setGZipCompression();

		pagingEnabled = true;
		tuplesPerPage = 50;
		pendingCompressionPackages = new ArrayList<>();
		serverResponseHandler = new HashMap<>();

		networkOperationRetryer = new NetworkOperationRetryer((p, f) -> {sendPackageToServer(p, f);});

		initResponseHandler();
	}

	/**
	 * Init the response handler
	 */
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
		serverResponseHandler.put(NetworkConst.RESPONSE_TYPE_JOINED_TUPLE, new JoinedTupleHandler());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#connect()
	 */
	@Override
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
			clientSocket = null;
			connectionState.dispatchToFailed(e);
			return false;
		} 

		return true;
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

		if(! connectionState.isInStartingState()) {
			logger.error("Handshaking called in wrong state: {}", connectionState);
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

		connectionState.dispatchToRunning();
		logger.debug("Handshaking with {} done", getConnectionName());

		mainteinanceHandler = new ConnectionMainteinanceThread(this);
		mainteinanceThread = new Thread(mainteinanceHandler);
		mainteinanceThread.setName("Connection mainteinace thread for: " + getConnectionName());
		mainteinanceThread.start();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#disconnect()
	 */
	@Override
	public void disconnect() {
		
		if(! connectionState.isInRunningState()) {
			logger.error("Unable to disconnect, connection is in state {}", connectionState);
			return;
		}

		synchronized (this) {
			logger.info("Disconnecting from server: {}", getConnectionName());
			connectionState.dispatchToStopping();

			final DisconnectRequest requestPackage = new DisconnectRequest(getNextSequenceNumber());
			final EmptyResultFuture operationFuture = new EmptyResultFuture(1);

			registerPackageCallback(requestPackage, operationFuture);
			sendPackageToServer(requestPackage, operationFuture);
		}

		settlePendingCalls(DEFAULT_TIMEOUT_MILLIS);

		terminateConnection();
	}

	/**
	 * Settle all pending calls
	 */
	public void settlePendingCalls(final long shutdownTimeMillis) {

		// Wait for all pending calls to settle
		synchronized (pendingCalls) {

			if(getInFlightCalls() == 0) {
				return;
			}

			logger.info("Waiting up to {} seconds for pending requests to settle", 
					TimeUnit.MILLISECONDS.toSeconds(shutdownTimeMillis));		

			final long shutdownStarted = System.currentTimeMillis();

			while(getInFlightCalls() > 0) {
				try {
					final long shutdownDuration = System.currentTimeMillis() - shutdownStarted;
					final long timeoutLeft = shutdownTimeMillis - shutdownDuration;

					if(timeoutLeft <= 0) {
						break;
					}

					pendingCalls.wait(timeoutLeft);
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
		if(connectionState.isInRunningState()) {
			connectionState.dispatchToStopping();
		}

		killPendingCalls();
		getResultBuffer().clear();

		CloseableHelper.closeWithoutException(clientSocket);
		clientSocket = null;

		logger.info("Disconnected from server");
		connectionState.forceDispatchToTerminated();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#createTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture createTable(final String table, final TupleStoreConfiguration configuration) throws BBoxDBException {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("createTable called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final CreateTableRequest requestPackage = new CreateTableRequest(getNextSequenceNumber(), table, configuration);
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTable(final String table) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("deleteTable called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final DeleteTableRequest requestPackage = new DeleteTableRequest(getNextSequenceNumber(), table);
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {

		try {
			if(! connectionState.isInRunningState()) {
				return FutureHelper.getFailedEmptyResultFuture("insertTuple called, but connection not ready: " + this);
			}

			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, tuple.getBoundingBox());

			return insertTuple(table, tuple, routingHeader);
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedEmptyResultFuture(e.getMessage());
		}
	}

	/**
	 * Get the routing header for the local system
	 * @param table
	 * @param tuple
	 * @return
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	protected RoutingHeader getRoutingHeaderForLocalSystem(final String table, final BoundingBox boundingBox)
			throws ZookeeperException, BBoxDBException, InterruptedException {

		final TupleStoreName ssTableName = new TupleStoreName(table);

		final SpacePartitioner spacepartitioner = SpacePartitionerCache.getSpaceParitionerForTableName(
				ssTableName);

		final DistributionRegion distributionRegion = spacepartitioner.getRootNode();

		final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForWrite(boundingBox, distributionRegion);

		// Filter the local hop
		final List<RoutingHop> connectionHop = hops.stream()
				.filter(r -> r.getDistributedInstance().getInetSocketAddress().equals(serverAddress))
				.collect(Collectors.toList());

		if(connectionHop.isEmpty()) {
			throw new BBoxDBException("Unable to find host " + serverAddress + " in global routing list: " 
					+ hops);
		}

		final RoutingHeader routingHeader = new RoutingHeader((short) 0, connectionHop);
		return routingHeader;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple, 
			final RoutingHeader routingHeader) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("insertTuple called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final TupleStoreName ssTableName = new TupleStoreName(table);
		final short sequenceNumber = getNextSequenceNumber();

		final InsertTupleRequest requestPackage = new InsertTupleRequest(
				sequenceNumber, 
				routingHeader, 
				ssTableName, 
				tuple);

		networkOperationRetryer.registerOperation(sequenceNumber, 
				requestPackage, clientOperationFuture);

		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("deleteTuple called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);

			final DeleteTupleRequest requestPackage = new DeleteTupleRequest(getNextSequenceNumber(), 
					routingHeader, table, key, timestamp);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);
			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedEmptyResultFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedEmptyResultFuture(e.getMessage());
		} 
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key) throws BBoxDBException {
		final long timestamp = MicroSecondTimestampProvider.getNewTimestamp();
		return deleteTuple(table, key, timestamp);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#listTables()
	 */
	@Override
	public SSTableNameListFuture listTables() {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedSStableNameFuture("listTables called, but connection not ready: " + this);
		}

		final SSTableNameListFuture clientOperationFuture = new SSTableNameListFuture(1);
		final ListTablesRequest requestPackage = new ListTablesRequest(getNextSequenceNumber());
		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#createDistributionGroup(java.lang.String, short)
	 */
	@Override
	public EmptyResultFuture createDistributionGroup(final String distributionGroup, 
			final DistributionGroupConfiguration distributionGroupConfiguration) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("listTables called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final CreateDistributionGroupRequest requestPackage = new CreateDistributionGroupRequest(
				getNextSequenceNumber(), distributionGroup, 
				distributionGroupConfiguration);

		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);
		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteDistributionGroup(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("delete distribution group called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final DeleteDistributionGroupRequest requestPackage = new DeleteDistributionGroupRequest(
				getNextSequenceNumber(), distributionGroup);

		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryKey(java.lang.String, java.lang.String)
	 */
	@Override
	public TupleListFuture queryKey(final String table, final String key) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryKey called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);
			
			final DuplicateResolver<Tuple> duplicateResolver 
				= TupleStoreConfigurationCache.getInstance().getDuplicateResolverForTupleStore(table);
			
			final TupleListFuture clientOperationFuture = new TupleListFuture(1, duplicateResolver);

			final QueryKeyRequest requestPackage = new QueryKeyRequest(getNextSequenceNumber(), routingHeader, 
					table, key, pagingEnabled, tuplesPerPage);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBox(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryBoundingBox(final String table, final BoundingBox boundingBox) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryBoundingBox called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);

			final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver());
			final QueryBoundingBoxRequest requestPackage = new QueryBoundingBoxRequest(getNextSequenceNumber(), 
					routingHeader, table, boundingBox, pagingEnabled, tuplesPerPage);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		}
	}

	/**
	 * Execute a continuous bounding box query
	 * 
	 */
	@Override
	public TupleListFuture queryBoundingBoxContinuous(final String table, final BoundingBox boundingBox) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryBoundingBoxContinuous called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);

			final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver());
			final QueryBoundingBoxContinuousRequest requestPackage = new QueryBoundingBoxContinuousRequest(getNextSequenceNumber(), 
					routingHeader, table, boundingBox);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBoxAndTime(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryBoundingBoxAndTime(final String table,
			final BoundingBox boundingBox, final long timestamp) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryBoundingBox called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);

			final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver());
			final QueryBoundingBoxTimeRequest requestPackage = new QueryBoundingBoxTimeRequest(getNextSequenceNumber(), 
					routingHeader, table, boundingBox, timestamp, pagingEnabled, tuplesPerPage);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryVersionTime(final String table, final long timestamp) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);

			final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver());
			final QueryVersionTimeRequest requestPackage = new QueryVersionTimeRequest(getNextSequenceNumber(), 
					routingHeader, table, timestamp, pagingEnabled, tuplesPerPage);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) {

		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(table, BoundingBox.EMPTY_BOX);

			final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver());
			final QueryInsertTimeRequest requestPackage = new QueryInsertTimeRequest(getNextSequenceNumber(), 
					routingHeader, table, timestamp, pagingEnabled, tuplesPerPage);

			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedTupleListFuture(e.getMessage());
		}
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryJoin
	 */
	@Override
	public JoinedTupleListFuture queryJoin(final List<String> tableNames, final BoundingBox boundingBox) {
		
		if(! connectionState.isInRunningState()) {
			return FutureHelper.getFailedJoinedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		try {
			final RoutingHeader routingHeader = getRoutingHeaderForLocalSystem(tableNames.get(0), BoundingBox.EMPTY_BOX);

			final JoinedTupleListFuture clientOperationFuture = new JoinedTupleListFuture(1);
			
			final List<TupleStoreName> tupleStoreNames = tableNames
					.stream()
					.map(t -> new TupleStoreName(t))
					.collect(Collectors.toList());
			
			final QueryJoinRequest requestPackage = new QueryJoinRequest(getNextSequenceNumber(), 
					routingHeader, tupleStoreNames, boundingBox, pagingEnabled, tuplesPerPage);
			
			registerPackageCallback(requestPackage, clientOperationFuture);
			sendPackageToServer(requestPackage, clientOperationFuture);

			// Send query immediately
			flushPendingCompressionPackages();

			return clientOperationFuture;
		} catch (BBoxDBException | ZookeeperException e) {
			// Return after exception
			return FutureHelper.getFailedJoinedTupleListFuture(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn("Interrupted while waiting for systems list");
			Thread.currentThread().interrupt();
			// Return after exception
			return FutureHelper.getFailedJoinedTupleListFuture(e.getMessage());
		}
	}

	/**
	 * Send a keep alive package to the server, to keep the TCP connection open.
	 * @return
	 */
	public EmptyResultFuture sendKeepAlivePackage() {

		synchronized (this) {
			if(! connectionState.isInRunningState()) {
				return FutureHelper.getFailedEmptyResultFuture("sendKeepAlivePackage called, but connection not ready: " + this);
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
			return FutureHelper.getFailedTupleListFuture(errorMessage);
		}

		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver());
		final NextPageRequest requestPackage = new NextPageRequest(
				getNextSequenceNumber(), queryPackageId);

		registerPackageCallback(requestPackage, clientOperationFuture);
		sendPackageToServer(requestPackage, clientOperationFuture);

		// Send query immediately
		flushPendingCompressionPackages();

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
	 * @see org.bboxdb.network.client.BBoxDB#isConnected()
	 */
	@Override
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
	@Override
	public int getInFlightCalls() {
		synchronized (pendingCalls) {
			return pendingCalls.size();
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#getMaxInFlightCalls()
	 */
	@Override
	public short getMaxInFlightCalls() {
		return maxInFlightCalls;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#setMaxInFlightCalls(short)
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

	/**
	 * Register a new package callback
	 * @param requestPackage
	 * @param future
	 * @return
	 */
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

	/**
	 * Get the result buffer
	 * @return
	 */
	public Map<Short, List<PagedTransferableEntity>> getResultBuffer() {
		return resultBuffer;
	}

	/**
	 * Get the network operation retryer
	 * @return
	 */
	public NetworkOperationRetryer getNetworkOperationRetryer() {
		return networkOperationRetryer;
	}
	
	/**
	 * Get the server response reader
	 * @return
	 */
	public ServerResponseReader getServerResponseReader() {
		return serverResponseReader;
	}
}

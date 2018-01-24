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
package org.bboxdb.network.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.commons.concurrent.ExecutorUtil;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.packages.NetworkResponsePackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.CompressionEnvelopeResponse;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.JoinedTupleResponse;
import org.bboxdb.network.packages.response.TupleResponse;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;
import org.bboxdb.network.server.handler.query.HandleBoundingBoxQuery;
import org.bboxdb.network.server.handler.query.HandleBoundingBoxTimeQuery;
import org.bboxdb.network.server.handler.query.HandleContinuousBoundingBoxQuery;
import org.bboxdb.network.server.handler.query.HandleInsertTimeQuery;
import org.bboxdb.network.server.handler.query.HandleJoinQuery;
import org.bboxdb.network.server.handler.query.HandleKeyQuery;
import org.bboxdb.network.server.handler.query.HandleVersionTimeQuery;
import org.bboxdb.network.server.handler.query.QueryHandler;
import org.bboxdb.network.server.handler.request.CancelQueryHandler;
import org.bboxdb.network.server.handler.request.CompressionHandler;
import org.bboxdb.network.server.handler.request.CreateDistributionGroupHandler;
import org.bboxdb.network.server.handler.request.CreateTableHandler;
import org.bboxdb.network.server.handler.request.DeleteDistributionGroupHandler;
import org.bboxdb.network.server.handler.request.DeleteTableHandler;
import org.bboxdb.network.server.handler.request.DeleteTupleHandler;
import org.bboxdb.network.server.handler.request.DisconnectHandler;
import org.bboxdb.network.server.handler.request.HandshakeHandler;
import org.bboxdb.network.server.handler.request.InsertTupleHandler;
import org.bboxdb.network.server.handler.request.KeepAliveHandler;
import org.bboxdb.network.server.handler.request.ListTablesHandler;
import org.bboxdb.network.server.handler.request.NextPageHandler;
import org.bboxdb.network.server.handler.request.RequestHandler;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import io.prometheus.client.Gauge;

public class ClientConnectionHandler extends ExceptionSafeThread {

	/**
	 * The client socket
	 */
	public final Socket clientSocket;
	
	/**
	 * The output stream of the socket
	 */
	protected BufferedOutputStream outputStream;
	
	/**
	 * The input stream of the socket
	 */
	protected InputStream inputStream;
	
	/**
	 * The connection state
	 */
	protected final ServiceState serviceState;

	/**
	 * The capabilities of the connection
	 */
	protected PeerCapabilities connectionCapabilities = new PeerCapabilities();
	
	/**
	 * The open query iterators, i.e., the queries that are not finished and waiting
	 * to send the next page
	 */
	private final Map<Short, ClientQuery> activeQueries;
	
	/**
	 * The thread pool
	 */
	private final ExecutorService threadPool;
	
	/**
	 * The package router
	 */
	private final PackageRouter packageRouter;
	
	/**
	 * The pending packages for compression
	 */
	protected final List<NetworkResponsePackage> pendingCompressionPackages;

	/**
	 * Number of pending requests
	 */
	protected final static int MAX_PENDING_REQUESTS = 25;

	/**
	 * Number of maximal running queries
	 */
	protected final static int MAX_RUNNING_QUERIES = 25;
	
	/**
	 * The request handlers
	 */
	protected Map<Short, RequestHandler> requestHandlers;

	/**
	 * The query handlers
	 */
	protected Map<Byte, QueryHandler> queryHandlerList;
	
	/**
	 * The connection maintenance thread
	 */
	protected ConnectionMaintenanceThread maintenanceThread;
	
	/**
	 * The storage reference
	 */
	protected final TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The read bytes counter
	 */
	protected final static Gauge readBytesCounter = Gauge.build()
			.name("bboxdb_network_read_bytes")
			.help("Total read bytes from network").register();
	
	/**
	 * The written bytes counter
	 */
	protected final static Gauge writtenBytesCounter = Gauge.build()
			.name("bboxdb_network_write_bytes")
			.help("Total written bytes to network").register();
	
	/**
	 * The active connections counter
	 */
	protected final static Gauge activeConnectionsTotal = Gauge.build()
			.name("bboxdb_network_connections_total")
			.help("Total amount of active network connections").register();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);

	public ClientConnectionHandler(final TupleStoreManagerRegistry storageRegistry, final Socket clientSocket) {
		
		// Client socket
		this.clientSocket = clientSocket;
		
		// The storage reference
		this.storageRegistry = storageRegistry;
		
		// Connection state set to starting until handshake is ready
		this.serviceState = new ServiceState();
		serviceState.registerCallback((s) -> { if(s.isInStartingState()) { activeConnectionsTotal.inc(); }});
		serviceState.registerCallback((s) -> { if(s.isInFinishedState()) { activeConnectionsTotal.dec(); }});

		serviceState.dipatchToStarting();
		
		try {
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
		} catch (IOException e) {
			inputStream = null;
			outputStream = null;
			serviceState.dispatchToFailed(e);
			logger.error("Exception while creating IO stream", e);
		}
		
		// The active queries
		activeQueries = new HashMap<>();
		
		// Create a thread pool that blocks after submitting more than MAX_PENDING_REQUESTS
		threadPool = ExecutorUtil.getBoundThreadPoolExecutor(10, MAX_PENDING_REQUESTS);

		// The package router
		packageRouter = new PackageRouter(threadPool, this);
		
		// The pending packages for compression 
		pendingCompressionPackages = new ArrayList<>();

		maintenanceThread = new ConnectionMaintenanceThread();
		final Thread thread = new Thread(maintenanceThread);
		thread.start();

		// Init the request handler map 
		initRequestHandlerMap();
		
		// Init the query handler map
		initQueryHandlerMap();
	}

	/**
	 * Read the next package header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	protected ByteBuffer readNextPackageHeader(final InputStream inputStream) throws IOException, PackageEncodeException {
		final ByteBuffer bb = ByteBuffer.allocate(12);
		ByteStreams.readFully(inputStream, bb.array(), 0, bb.limit());
		
		final RoutingHeader routingHeader = RoutingHeaderParser.decodeRoutingHeader(inputStream);
		final byte[] routingHeaderBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteBuffer header = ByteBuffer.allocate(bb.limit() + routingHeaderBytes.length);
		header.put(bb.array());
		header.put(routingHeaderBytes);
		
		return header;
	}

	/**
	 * Write all pending compression packages to server, called by the maintainance thread
	 * 
	 */
	protected void flushPendingCompressionPackages() {
		
		final List<NetworkResponsePackage> packagesToWrite = new ArrayList<>();
		
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
		
		final NetworkResponsePackage compressionEnvelopeRequest 
			= new CompressionEnvelopeResponse(NetworkConst.COMPRESSION_TYPE_GZIP, packagesToWrite);
		
		try {
			writePackageToSocket(compressionEnvelopeRequest);
		} catch (PackageEncodeException | IOException e) {
			logger.error("Got an exception while write pending compression packages to client", e);
		}
	}
	
	/**
	 * Write a response package to the client
	 * @param responsePackage
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public synchronized void writeResultPackage(final NetworkResponsePackage responsePackage) 
			throws IOException, PackageEncodeException {
		
		if(connectionCapabilities.hasGZipCompression()) {
			boolean uncompressedQueueFull = false;
			
			synchronized (pendingCompressionPackages) {
				// Schedule for batch compression
				pendingCompressionPackages.add(responsePackage);
				uncompressedQueueFull = pendingCompressionPackages.size() >= Const.MAX_UNCOMPRESSED_QUEUE_SIZE;
			}
			
			if(uncompressedQueueFull) {
				flushPendingCompressionPackages();
			}
			
		} else {
			writePackageToSocket(responsePackage);
		}
	}
	
	/**
	 * Send a package and catch the exception
	 * @param responsePackage
	 */
	public synchronized void writeResultPackageNE(final NetworkResponsePackage responsePackage) {
		try {
			writeResultPackage(responsePackage);
		} catch (Exception e) {
			logger.error("Unable to send package", e);
		}
	}

	/**
	 * Write a network package uncompressed
	 * @param responsePackage
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	protected void writePackageToSocket(final NetworkResponsePackage responsePackage) 
			throws IOException, PackageEncodeException {
		
		synchronized (outputStream) {
			final long writtenBytes = responsePackage.writeToOutputStream(outputStream);
			writtenBytesCounter.inc(writtenBytes);
			outputStream.flush();
		}
	}
	
	@Override
	public void runThread() {
		try {
			logger.debug("Handling new connection from: " + clientSocket.getInetAddress());

			while(serviceState.isInRunningState() || serviceState.isInStartingState()) {
				handleNextPackage(inputStream);
			}

			// Flush all pending results to client
			flushPendingCompressionPackages();
			
			// Conenction is down
			if(! serviceState.isInStoppingState()) {
				serviceState.dispatchToStopping();
			}
			
			serviceState.dispatchToTerminated();
			
			logger.info("Closing connection to: {}", clientSocket.getInetAddress());
		} catch (IOException | PackageEncodeException e) {
			// Ignore exception on closing sockets
			if(serviceState.isInRunningState()) {
				
				logger.error("Socket to {} closed unexpectly (state: {}), closing connection",
						clientSocket.getInetAddress(), getConnectionState());

				logger.debug("Socket exception", e);
			}
		} 
		
		getThreadPool().shutdown();
		
		// Close active query iterators
		getActiveQueries().values().forEach(i -> i.close());
		getActiveQueries().clear();	
		
		CloseableHelper.closeWithoutException(clientSocket);
	}
	
	/**
	 * Read the full package. The total length of the package is read from the package header.
	 * @param packageHeader
	 * @return
	 * @throws IOException 
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader, 
			final InputStream inputStream) throws IOException {
		
		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final int headerLength = packageHeader.limit();
		final int packageLength = headerLength + bodyLength;
		
		final ByteBuffer encodedPackage = ByteBuffer.allocate(packageLength);
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + in.available());			
			encodedPackage.put(packageHeader.array());
			ByteStreams.readFully(inputStream, encodedPackage.array(), encodedPackage.position(), bodyLength);
			readBytesCounter.inc(packageLength);
		} catch (IOException e) {
			serviceState.dispatchToStopping();
			throw e;
		}
		 
		return encodedPackage;
	}

	/**
	 * Handle the next request package
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	public void handleNextPackage(final InputStream inputStream) throws IOException, PackageEncodeException {
		final ByteBuffer packageHeader = readNextPackageHeader(inputStream);
		
		final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(packageHeader);
		final short packageType = NetworkPackageDecoder.getPackageTypeFromRequest(packageHeader);
		
		if(serviceState.isInStartingState()) {
			if(packageType != NetworkConst.REQUEST_TYPE_HELLO) {
				final String errorMessage = "Connection is in handshake state but got package: " + packageType;
				logger.error(errorMessage);
				serviceState.dispatchToFailed(new IllegalStateException(errorMessage));
				return;
			}
		}
		
		final ByteBuffer encodedPackage = readFullPackage(packageHeader, inputStream);

		final boolean readFurtherPackages = handleBufferedPackage(encodedPackage, packageSequence, packageType);

		if(readFurtherPackages == false) {
			serviceState.dispatchToStopping();
		}	
	}

	/**
	 * Send a new result tuple to the client
	 * @param packageSequence
	 * @param requestTable
	 * @param tuple
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public void writeResultTuple(final short packageSequence, final JoinedTuple joinedTuple) 
			throws IOException, PackageEncodeException {
		
		if(joinedTuple.getNumberOfTuples() == 1) {
			final TupleResponse responsePackage = new TupleResponse(
					packageSequence, 
					joinedTuple.getTupleStoreName(0), 
					joinedTuple.getTuple(0));
			
			writeResultPackage(responsePackage);
		} else {
			final JoinedTupleResponse responsePackage = new JoinedTupleResponse(
					packageSequence, joinedTuple);
			
			writeResultPackage(responsePackage);
		}
		
	}

	/**
	 * Handle query package
	 * @param bb
	 * @param packageSequence
	 * @return
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	protected boolean handleQuery(final ByteBuffer encodedPackage, final short packageSequence) 
			throws IOException, PackageEncodeException {
	
		final byte queryType = NetworkPackageDecoder.getQueryTypeFromRequest(encodedPackage);

		if(! queryHandlerList.containsKey(queryType)) {
			logger.warn("Unsupported query type: {}", queryType);
			
			final ErrorResponse errorResponse = new ErrorResponse(packageSequence, 
					ErrorMessages.ERROR_UNSUPPORTED_PACKAGE_TYPE);
			
			writeResultPackage(errorResponse);
			return false;
		}
		
		if(activeQueries.size() > MAX_RUNNING_QUERIES) {
			logger.warn("Client requested more than {} parallel queries", MAX_RUNNING_QUERIES);
			final ErrorResponse errorResponse = new ErrorResponse(packageSequence, 
					ErrorMessages.ERROR_QUERY_TO_MUCH);
			
			writeResultPackage(errorResponse);
		} else {
			final QueryHandler queryHandler = queryHandlerList.get(queryType);
			queryHandler.handleQuery(encodedPackage, packageSequence, this);
		}
		
		return true;
	}
	
	/**
	 * Handle a buffered package
	 * @param encodedPackage
	 * @param packageSequence
	 * @param packageType
	 * @return
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	protected boolean handleBufferedPackage(final ByteBuffer encodedPackage, 
			final short packageSequence, 
			final short packageType) throws PackageEncodeException, IOException {
				
		// Handle the different query types
		if(packageType == NetworkConst.REQUEST_TYPE_QUERY) {
			if(logger.isDebugEnabled()) {
				logger.debug("Got query package");
			}
			
			return handleQuery(encodedPackage, packageSequence);
		}

		if(requestHandlers.containsKey(packageType)) {
			final RequestHandler requestHandler = requestHandlers.get(packageType);
			
			if(logger.isDebugEnabled()) {
				logger.debug("Dispatching package to handler: {}", requestHandler);
			}
			
			final boolean handleFurtherPackages 
				= requestHandler.handleRequest(encodedPackage, packageSequence, this);
			
			return handleFurtherPackages;
		} else {
			logger.error("Got unknown package type, closing connection: " + packageType);
			serviceState.dispatchToStopping();
			return false;
		}
	}

	/**
	 * Init the map with the package handlers
	 * @return
	 */
	protected void initRequestHandlerMap() {
		requestHandlers = new HashMap<>();
		requestHandlers.put(NetworkConst.REQUEST_TYPE_HELLO, new HandshakeHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_COMPRESSION, new CompressionHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_DISCONNECT, new DisconnectHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_CREATE_TABLE, new CreateTableHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_DELETE_TABLE, new DeleteTableHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_DELETE_TUPLE, new DeleteTupleHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_LIST_TABLES, new ListTablesHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_INSERT_TUPLE, new InsertTupleHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP, new CreateDistributionGroupHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP, new DeleteDistributionGroupHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_KEEP_ALIVE, new KeepAliveHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_NEXT_PAGE, new NextPageHandler());
		requestHandlers.put(NetworkConst.REQUEST_TYPE_CANCEL_QUERY, new CancelQueryHandler());
	}
	
	/**
	 * Init the query handler map
	 */
	protected void initQueryHandlerMap() {
		queryHandlerList = new HashMap<>();
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_KEY, new HandleKeyQuery());
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_BBOX, new HandleBoundingBoxQuery());
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_VERSION_TIME, new HandleVersionTimeQuery());
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_INSERT_TIME, new HandleInsertTimeQuery());
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_BBOX_AND_TIME, new HandleBoundingBoxTimeQuery());
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_CONTINUOUS_BBOX, new HandleContinuousBoundingBoxQuery());
		queryHandlerList.put(NetworkConst.REQUEST_QUERY_JOIN, new HandleJoinQuery());
	}

	/**
	 * Send next results for the given query
	 * @param packageSequence
	 * @param  
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public void sendNextResultsForQuery(final short packageSequence, final short querySequence) 
			throws IOException, PackageEncodeException {
			
		if(! getActiveQueries().containsKey(querySequence)) {
			logger.error("Unable to resume query {} - package {} - not found", querySequence, packageSequence);
			writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_NOT_FOUND));
			return;
		}
		
		final Runnable queryRunable = new ExceptionSafeThread() {

			@Override
			protected void runThread() throws IOException, PackageEncodeException {
				final ClientQuery clientQuery = getActiveQueries().get(querySequence);
				clientQuery.fetchAndSendNextTuples(packageSequence);
				
				if(clientQuery.isQueryDone()) {
					logger.info("Query {} is done with {} tuples, removing iterator ", 
							querySequence,
							clientQuery.getTotalSendTuples());
					clientQuery.close();
					getActiveQueries().remove(querySequence);
				}
			}
			
			@Override
			protected void afterExceptionHook() {
				try {
					writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));
				} catch (IOException | PackageEncodeException e) {
					logger.error("Unable to send result package", e);
				}
			}
		};

		// Submit the runnable to our pool
		if(threadPool.isShutdown()) {
			logger.warn("Thread pool is shutting down, don't execute query: {}", querySequence);
			writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));
		} else {
			getThreadPool().submit(queryRunable);
		}
	}
	
	/**
	 * Get the connection Capabilities
	 * @return
	 */
	public PeerCapabilities getConnectionCapabilities() {
		return connectionCapabilities;
	}

	/**
	 * Set the connection Capabilities
	 * @return
	 */
	public void setConnectionCapabilities(PeerCapabilities connectionCapabilities) {
		this.connectionCapabilities = connectionCapabilities;
	}

	public ServiceState getConnectionState() {
		return serviceState;
	}

	public void setConnectionStateToOpen() {
		serviceState.dispatchToRunning();
	}

	public Map<Short, ClientQuery> getActiveQueries() {
		return activeQueries;
	}

	public ExecutorService getThreadPool() {
		return threadPool;
	}

	public PackageRouter getPackageRouter() {
		return packageRouter;
	}
	
	/**
	 * Get the name of the connection
	 * @return
	 */
	public String getConnectionName() {
		
		final StringBuilder sb = new StringBuilder("Connection: ");

		if(clientSocket != null) {
			sb.append("Client: ");
			if(clientSocket.getRemoteSocketAddress() != null) {
				sb.append(clientSocket.getRemoteSocketAddress().toString());
			} else {
				sb.append("-");
			}
			
			sb.append(" to: ");
			if(clientSocket.getLocalAddress() != null) {
				sb.append(clientSocket.getLocalAddress().toString());
			} else {
				sb.append("-");
			}
		}
		
		return sb.toString();
	}
	
	class ConnectionMaintenanceThread extends ExceptionSafeThread {
		
		@Override
		protected void beginHook() {
			logger.debug("Starting connection mainteinance thread for: {}", getConnectionName());
		}
		
		@Override
		protected void endHook() {
			logger.debug("Mainteinance thread for {} has terminated", getConnectionName());
		}

		@Override
		protected void runThread() throws Exception {
			while(serviceState.isInStartingState() || serviceState.isInRunningState()) {
				
				// Write all waiting for compression packages
				flushPendingCompressionPackages();
		
				try {
					Thread.sleep(NetworkConst.MAX_COMPRESSION_DELAY_MS);
				} catch (InterruptedException e) {
					// Handle InterruptedException directly
					return;
				}
			}
		}
		
	};
	
	/**
	 * Get the storage registry
	 * @return
	 */
	public TupleStoreManagerRegistry getStorageRegistry() {
		return storageRegistry;
	}
	
	/**
	 * The connection shutdown handler
	 * @param handler
	 */
	public void addConnectionClosedHandler(final Consumer<ClientConnectionHandler> handler) {
		
		serviceState.registerCallback(c -> {
			if(c.isInFinishedState()) { 
				handler.accept(this); 
			}
		});
		
	}
}
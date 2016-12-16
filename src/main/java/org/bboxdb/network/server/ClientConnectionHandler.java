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
package org.bboxdb.network.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.mode.NodeState;
import org.bboxdb.distribution.nameprefix.NameprefixInstanceManager;
import org.bboxdb.distribution.nameprefix.NameprefixMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkHelper;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.packages.NetworkResponsePackage;
import org.bboxdb.network.packages.PackageEncodeError;
import org.bboxdb.network.packages.request.CancelQueryRequest;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.DeleteTupleRequest;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxTimeRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryTimeRequest;
import org.bboxdb.network.packages.request.TransferSSTableRequest;
import org.bboxdb.network.packages.response.CompressionEnvelopeResponse;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.packages.response.ListTablesResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.packages.response.TupleResponse;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.predicate.AndPredicate;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.OverlapsBoundingBoxPredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConnectionHandler implements Runnable {

	/**
	 * The client socket
	 */
	protected final Socket clientSocket;
	
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
	protected volatile NetworkConnectionState connectionState;

	/**
	 * The thread pool
	 */
	protected final ThreadPoolExecutor threadPool;
	
	/**
	 * The package router
	 */
	protected final PackageRouter packageRouter;
	
	/**
	 * The state of the server (readonly or readwrite)
	 */
	protected final NetworkConnectionServiceState networkConnectionServiceState;
	
	/**
	 * The capabilities of the connection
	 */
	protected PeerCapabilities connectionCapabilities = new PeerCapabilities();
	
	/**
	 * The open query iterators, i.e., the queries that are not finished and waiting
	 * to send the next page
	 */
	protected final Map<Short, ClientQuery> activeQueries;
	
	/**
	 * Number of pending requests
	 */
	public final static int PENDING_REQUESTS = 25;
	
	/**
	 * Readony node error message
	 */
	protected final static String INSTANCE_IS_READ_ONLY_MSG = "Instance is read only";

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);

	public ClientConnectionHandler(final Socket clientSocket, final NetworkConnectionServiceState networkConnectionServiceState) {
		
		// The network connection state
		this.clientSocket = clientSocket;
		this.networkConnectionServiceState = networkConnectionServiceState;
		
		this.connectionState = NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING;
		
		// Create a thread pool that blocks after submitting more than PENDING_REQUESTS
		final BlockingQueue<Runnable> linkedBlockingDeque = new LinkedBlockingDeque<Runnable>(PENDING_REQUESTS);
		threadPool = new ThreadPoolExecutor(1, PENDING_REQUESTS/2, 30, TimeUnit.SECONDS, 
				linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy());
		
		// The package router
		packageRouter = new PackageRouter(threadPool, this);
		
		// The open queries
		activeQueries = new HashMap<Short, ClientQuery>();
		
		try {
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
		} catch (IOException e) {
			inputStream = null;
			outputStream = null;
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.error("Exception while creating IO stream", e);
		}
	}

	/**
	 * Read the next package header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 */
	protected ByteBuffer readNextPackageHeader() throws IOException {
		final ByteBuffer bb = ByteBuffer.allocate(12);
		NetworkHelper.readExactlyBytes(inputStream, bb.array(), 0, bb.limit());
		
		final RoutingHeader routingHeader = RoutingHeaderParser.decodeRoutingHeader(inputStream);
		final byte[] routingHeaderBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteBuffer header = ByteBuffer.allocate(bb.limit() + routingHeaderBytes.length);
		header.put(bb.array());
		header.put(routingHeaderBytes);
		
		return header;
	}

	/**
	 * Write a response package to the client
	 * @param responsePackage
	 */
	public synchronized boolean writeResultPackage(final NetworkResponsePackage responsePackage) {
		
		try {
			final byte[] outputData = responsePackage.getByteArray();

			outputStream.write(outputData, 0, outputData.length);
			outputStream.flush();
			return true;
		} catch (IOException | PackageEncodeError e) {
			logger.warn("Unable to write result package", e);
		}

		return false;
	}
	
	@Override
	public void run() {
		try {
			logger.debug("Handling new connection from: " + clientSocket.getInetAddress());

			while(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN ||
					connectionState == NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING) {
				handleNextPackage();
			}
			
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.info("Closing connection to: " + clientSocket.getInetAddress());
		} catch (IOException e) {
			// Ignore exception on closing sockets
			if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				logger.error("Socket closed unexpectly (state: " + connectionState + "), closing connection", e);
			}
		} catch(Throwable e) {
			logger.error("Got an exception during connection handling: ", e);
		}
		
		threadPool.shutdown();
		
		// Close active query iterators
		for(final ClientQuery clientQuery : activeQueries.values()) {
			clientQuery.close();
		}
		activeQueries.clear();
		
		closeSocketNE();
	}

	/**
	 * Close the socket without throwing an exception
	 */
	protected void closeSocketNE() {
		try {
			clientSocket.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}
	
	/**
	 * Handle query package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleQuery(final ByteBuffer encodedPackage, final short packageSequence) {
	
		final byte queryType = NetworkPackageDecoder.getQueryTypeFromRequest(encodedPackage);

		switch (queryType) {
			case NetworkConst.REQUEST_QUERY_KEY:
				handleKeyQuery(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_QUERY_BBOX:
				handleBoundingBoxQuery(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_QUERY_TIME:
				handleTimeQuery(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_QUERY_BBOX_AND_TIME:
				handleBoundingBoxTimeQuery(encodedPackage, packageSequence);
				break;
	
			default:
				logger.warn("Unsupported query type: " + queryType);
				writeResultPackage(new ErrorResponse(packageSequence));
				return true;
		}

		return true;
	}
	
	/**
	 * Handle the transfer package. In contrast to other packages, this package
	 * type can become very large. Therefore, the data is not buffered into a byte 
	 * buffer. The network stream is directly passed to the decoder.
	 * 
	 * @param bb
	 * @param packageSequence
	 * @return
	 * @throws PackageEncodeError 
	 */
	protected boolean handleTransfer(final ByteBuffer packageHeader, final short packageSequence) throws PackageEncodeError {
		
		final long bodyLength = NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		
		try {
			TransferSSTableRequest.decodeTuple(packageHeader, bodyLength, configuration, inputStream);
		} catch (IOException e) {
			logger.warn("Exception while handling sstable transfer", e);
		}
		
		return true;
	}
	
	/**
	 * Create a new distribution group
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleCreateDistributionGroup(final ByteBuffer encodedPackage, final short packageSequence) {
		
		try {
			if(networkConnectionServiceState.isReadonly()) {
				writeResultPackage(new ErrorResponse(packageSequence, INSTANCE_IS_READ_ONLY_MSG));
				return true;
			}
			
			final CreateDistributionGroupRequest createPackage = CreateDistributionGroupRequest.decodeTuple(encodedPackage);
			logger.info("Create distribution group: " + createPackage.getDistributionGroup());
			
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			
			distributionGroupZookeeperAdapter.createDistributionGroup(createPackage.getDistributionGroup(), createPackage.getReplicationFactor());
			
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForGroupName(
					createPackage.getDistributionGroup(), zookeeperClient);

			final DistributionRegion region = distributionAdapter.getAndWaitForRootNode();
			
			DistributionRegionHelper.allocateSystemsToNewRegion(region, zookeeperClient);
			
			// Let the data settle down
			Thread.sleep(5000);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(region, NodeState.ACTIVE);
			
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while create distribution group", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}
	
	/**
	 * Handle the keep alive package. Simply send a success response package back
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleKeepAlivePackage(final ByteBuffer encodedPackage, final short packageSequence) {
		writeResultPackage(new SuccessResponse(packageSequence));
		
		return true;
	}
	
	
	/**
	 * Handle next page package
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleNextPagePackage(final ByteBuffer encodedPackage, final short packageSequence) {

		try {
			final NextPageRequest nextPagePackage = NextPageRequest.decodeTuple(encodedPackage);
			logger.debug("Next page for query {} called", nextPagePackage.getQuerySequence());
			sendNextResultsForQuery(nextPagePackage.getQuerySequence());
		} catch (PackageEncodeError e) {
			logger.warn("Error getting next page for a query", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}
	
	/**
	 * Cancel the given query
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleCancelQueryPackage(final ByteBuffer encodedPackage, final short packageSequence) {

		try {
			final CancelQueryRequest nextPagePackage = CancelQueryRequest.decodeTuple(encodedPackage);
			logger.debug("Cancel query {} requested", nextPagePackage.getQuerySequence());
			
			if(! activeQueries.containsKey(packageSequence)) {
				logger.error("Unable to cancel query {} - not found", packageSequence);
				writeResultPackage(new ErrorResponse(packageSequence, "Query to cancel not found"));
			} else {
				final ClientQuery clientQuery = activeQueries.remove(packageSequence);
				clientQuery.close();
				writeResultPackage(new SuccessResponse(packageSequence));
			}
		} catch (PackageEncodeError e) {
			logger.warn("Error getting next page for a query", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}
		
	
	/**
	 * Delete an existing distribution group
	 * @param packageHeader
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleDeleteDistributionGroup(final ByteBuffer encodedPackage, final short packageSequence) {
		
		try {			
			if(networkConnectionServiceState.isReadonly()) {
				writeResultPackage(new ErrorResponse(packageSequence, INSTANCE_IS_READ_ONLY_MSG));
				return true;
			}
			
			final DeleteDistributionGroupRequest deletePackage = DeleteDistributionGroupRequest.decodeTuple(encodedPackage);
			logger.info("Delete distribution group: " + deletePackage.getDistributionGroup());
			
			// Delete in Zookeeper
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			distributionGroupZookeeperAdapter.deleteDistributionGroup(deletePackage.getDistributionGroup());
			
			// Delete local stored data
			final DistributionGroupName distributionGroupName = new DistributionGroupName(deletePackage.getDistributionGroup());
			StorageRegistry.deleteAllTablesInDistributionGroup(distributionGroupName);
			
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while delete distribution group", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}
	
	/**
	 * Run the connection handshake
	 * @param packageSequence 
	 * @return
	 */
	protected boolean runHandshake(final ByteBuffer encodedPackage, final short packageSequence) {
		try {	
			final HelloRequest heloRequest = HelloRequest.decodeRequest(encodedPackage);
			connectionCapabilities = heloRequest.getPeerCapabilities();

			writeResultPackage(new HelloResponse(packageSequence, NetworkConst.PROTOCOL_VERSION, connectionCapabilities));

			connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
			return true;
		} catch(Exception e) {
			logger.warn("Error while reading network package", e);
			writeResultPackage(new ErrorResponse(packageSequence));
			return false;
		}
	}
	
	/**
	 * Handle compressed packages. Uncompress envelope and handle package
	 * @param encodedPackage
	 * @param packageSequence
	 * @return
	 * @throws PackageEncodeError 
	 */
	protected boolean handleCompression(final ByteBuffer encodedPackage, final short packageSequence) throws PackageEncodeError {

		final byte[] uncompressedPackage = CompressionEnvelopeRequest.decodePackage(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(uncompressedPackage);

		final short packageType = NetworkPackageDecoder.getPackageTypeFromRequest(bb);
		handleBufferedPackage(bb, packageSequence, packageType);
		
		return true;
	}
	
	/**
	 * Handle the delete table call
	 * @param packageSequence 
	 * @return
	 * @throws PackageEncodeError 
	 */
	protected boolean handleDeleteTable(final ByteBuffer encodedPackage, final short packageSequence) throws PackageEncodeError {
		
		try {
			if(networkConnectionServiceState.isReadonly()) {
				writeResultPackage(new ErrorResponse(packageSequence, INSTANCE_IS_READ_ONLY_MSG));
				return true;
			}
			
			final DeleteTableRequest deletePackage = DeleteTableRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = deletePackage.getTable();
			logger.info("Got delete call for table: " + requestTable);
			
			// Send the call to the storage manager
			final NameprefixMapper nameprefixManager = NameprefixInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final Collection<SSTableName> localTables = nameprefixManager.getAllNameprefixesWithTable(requestTable);
			
			for(final SSTableName ssTableName : localTables) {
				StorageRegistry.deleteTable(ssTableName);	
			}
			
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}

	/**
	 * Handle a key query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleKeyQuery(final ByteBuffer encodedPackage, final short packageSequence) {

		final Runnable queryRunable = new Runnable() {

			@Override
			public void run() {

				try {
					executeThread(encodedPackage, packageSequence);
				} catch (Throwable e) {
					logger.warn("Got exception while scanning for key", e);
					writeResultPackage(new ErrorResponse(packageSequence));				
					return;
				}
				
			}

			/**
			 * Execute the thread
			 * @param encodedPackage
			 * @param packageSequence
			 * @throws PackageEncodeError
			 * @throws StorageManagerException
			 */
			protected void executeThread(final ByteBuffer encodedPackage,
					final short packageSequence) throws PackageEncodeError,
					StorageManagerException {
				final QueryKeyRequest queryKeyRequest = QueryKeyRequest.decodeTuple(encodedPackage);
				final SSTableName requestTable = queryKeyRequest.getTable();
				
				// Send the call to the storage manager
				final NameprefixMapper nameprefixManager = NameprefixInstanceManager.getInstance(requestTable.getDistributionGroupObject());
				final Collection<SSTableName> localTables = nameprefixManager.getAllNameprefixesWithTable(requestTable);
				
				for(final SSTableName ssTableName : localTables) {
					final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
					final Tuple tuple = storageManager.get(queryKeyRequest.getKey());
					
					if(tuple != null) {
						writeResultTuple(packageSequence, requestTable, tuple);
						return;
					}
				}

				writeResultPackage(new SuccessResponse(packageSequence));
				return;
			}			
		};

		// Submit the runnable to our pool
		if(threadPool.isTerminating()) {
			logger.warn("Thread pool is shutting down, don't execute query: {}", packageSequence);
			writeResultPackage(new ErrorResponse(packageSequence));
		} else {
			threadPool.submit(queryRunable);
		}
	}
	
	/**
	 * Handle a bounding box query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleBoundingBoxQuery(final ByteBuffer encodedPackage, final short packageSequence) {

		try {
			if(activeQueries.containsKey(packageSequence)) {
				logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
				return;
			}
			
			final QueryBoundingBoxRequest queryRequest = QueryBoundingBoxRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = queryRequest.getTable();
			final Predicate predicate = new OverlapsBoundingBoxPredicate(queryRequest.getBoundingBox());

			final ClientQuery clientQuery = new ClientQuery(predicate, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), this, packageSequence, requestTable);
			
			activeQueries.put(packageSequence, clientQuery);
			sendNextResultsForQuery(packageSequence);
		} catch (PackageEncodeError e) {
			logger.warn("Got exception while decoding package", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
	}
	
	/**
	 * Handle a bounding box query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleBoundingBoxTimeQuery(final ByteBuffer encodedPackage, final short packageSequence) {

		try {
			if(activeQueries.containsKey(packageSequence)) {
				logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
				return;
			}
			
			final QueryBoundingBoxTimeRequest queryRequest = QueryBoundingBoxTimeRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = queryRequest.getTable();
	
			final Predicate bboxPredicate = new OverlapsBoundingBoxPredicate(queryRequest.getBoundingBox());
			final Predicate timePredicate = new NewerAsTimePredicate(queryRequest.getTimestamp());
			final Predicate predicate = new AndPredicate(timePredicate, bboxPredicate);
			
			final ClientQuery clientQuery = new ClientQuery(predicate, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), this, packageSequence, requestTable);
			
			activeQueries.put(packageSequence, clientQuery);
			sendNextResultsForQuery(packageSequence);
		} catch (PackageEncodeError e) {
			logger.warn("Got exception while decoding package", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
	}
	
	/**
	 * Send next results for the given query
	 * @param packageSequence
	 */
	public void sendNextResultsForQuery(final short packageSequence) {
		
		if(! activeQueries.containsKey(packageSequence)) {
			logger.error("Unable to resume query {} - not found", packageSequence);
			writeResultPackage(new ErrorResponse(packageSequence));
			return;
		}
		
		final Runnable queryRunable = new Runnable() {

			@Override
			public void run() {
				try {
					executeThread(packageSequence);
				} catch(Throwable e) {
					logger.error("Got uncought exception in thread", e);
					writeResultPackage(new ErrorResponse(packageSequence));
					return;
				}
			}

			/**
			 * Execute the thread
			 * @param packageSequence
			 */
			protected void executeThread(final short packageSequence) {
				final ClientQuery clientQuery = activeQueries.get(packageSequence);
				clientQuery.fetchAndSendNextTuples();
				
				if(clientQuery.isQueryDone()) {
					logger.debug("Query {} is done, closing and removing iterator", packageSequence);
					clientQuery.close();
					activeQueries.remove(packageSequence);
				}
			}

		};

		// Submit the runnable to our pool
		if(threadPool.isTerminating()) {
			logger.warn("Thread pool is shutting down, don't execute query: {}", packageSequence);
			writeResultPackage(new ErrorResponse(packageSequence));
		} else {
			threadPool.submit(queryRunable);
		}
	}
	
	/**
	 * Handle a time query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleTimeQuery(final ByteBuffer encodedPackage, final short packageSequence) {
		
		try {
			if(activeQueries.containsKey(packageSequence)) {
				logger.error("Query sequence {} is allready known, please close old query first", packageSequence);
				return;
			}
			
			final QueryTimeRequest queryRequest = QueryTimeRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = queryRequest.getTable();
			final Predicate predicate = new NewerAsTimePredicate(queryRequest.getTimestamp());

			final ClientQuery clientQuery = new ClientQuery(predicate, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), this, packageSequence, requestTable);
			
			activeQueries.put(packageSequence, clientQuery);
			sendNextResultsForQuery(packageSequence);
		} catch (PackageEncodeError e) {
			logger.warn("Got exception while decoding package", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
	}

	/**
	 * Handle Insert tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleInsertTuple(final ByteBuffer encodedPackage, final short packageSequence) {
		
		try {
			if(networkConnectionServiceState.isReadonly()) {
				writeResultPackage(new ErrorResponse(packageSequence, INSTANCE_IS_READ_ONLY_MSG));
				return true;
			}
			
			final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage);
			
			// Send the call to the storage manager
			final Tuple tuple = insertTupleRequest.getTuple();			
			final SSTableName requestTable = insertTupleRequest.getTable();
			
			final NameprefixMapper nameprefixManager = NameprefixInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final BoundingBox boundingBox = insertTupleRequest.getTuple().getBoundingBox();
			final Collection<SSTableName> localTables = nameprefixManager.getNameprefixesForRegionWithTable(boundingBox, requestTable);

			for(final SSTableName ssTableName : localTables) {
				final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
				storageManager.put(tuple);
			}

			packageRouter.performInsertPackageRoutingAsync(packageSequence, insertTupleRequest, boundingBox);
			
		} catch (Exception e) {
			logger.warn("Error while insert tuple", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		}
		
		return true;
	}

	/**
	 * Handle list tables package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleListTables(final ByteBuffer encodedPackage, final short packageSequence) {
		final List<SSTableName> allTables = StorageRegistry.getAllTables();
		final ListTablesResponse listTablesResponse = new ListTablesResponse(packageSequence, allTables);
		writeResultPackage(listTablesResponse);
		
		return true;
	}

	/**
	 * Handle delete tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 * @throws PackageEncodeError 
	 */
	protected boolean handleDeleteTuple(final ByteBuffer encodedPackage, final short packageSequence) throws PackageEncodeError {

		try {
			if(networkConnectionServiceState.isReadonly()) {
				writeResultPackage(new ErrorResponse(packageSequence, INSTANCE_IS_READ_ONLY_MSG));
				return true;
			}
			
			final DeleteTupleRequest deleteTupleRequest = DeleteTupleRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = deleteTupleRequest.getTable();

			// Send the call to the storage manager
			final NameprefixMapper nameprefixManager = NameprefixInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final Collection<SSTableName> localTables = nameprefixManager.getAllNameprefixesWithTable(requestTable);

			for(final SSTableName ssTableName : localTables) {
				final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
				storageManager.delete(deleteTupleRequest.getKey(), deleteTupleRequest.getTimestamp());
			}
			
			writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			writeResultPackage(new ErrorResponse(packageSequence));	
		} 

		return true;
	}	
	
	/**
	 * Handle the disconnect request
	 * @param encodedPackage
	 * @return
	 */
	protected boolean handleDisconnect(final ByteBuffer encodedPackage, final short packageSequence) {
		writeResultPackage(new SuccessResponse(packageSequence));
		return false;
	}

	/**
	 * Read the full package. The total length of the package is read from the package header.
	 * @param packageHeader
	 * @return
	 * @throws IOException 
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader) throws IOException {
		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final int headerLength = packageHeader.limit();
		
		final ByteBuffer encodedPackage = ByteBuffer.allocate(headerLength + bodyLength);
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + in.available());			
			encodedPackage.put(packageHeader.array());
			NetworkHelper.readExactlyBytes(inputStream, encodedPackage.array(), encodedPackage.position(), bodyLength);
		} catch (IOException e) {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
			throw e;
		}
		
		return encodedPackage;
	}

	/**
	 * Handle the next request package
	 * @throws IOException
	 * @throws PackageEncodeError 
	 */
	protected void handleNextPackage() throws IOException, PackageEncodeError {
		final ByteBuffer packageHeader = readNextPackageHeader();

		final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(packageHeader);
		final short packageType = NetworkPackageDecoder.getPackageTypeFromRequest(packageHeader);
		
		if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING) {
			if(packageType != NetworkConst.REQUEST_TYPE_HELLO) {
				logger.error("Connection is in handshake state but got package: " + packageType);
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				return;
			}
		}
		
		boolean readFurtherPackages = true;
		
		if(packageType == NetworkConst.REQUEST_TYPE_TRANSFER) {
			if(logger.isDebugEnabled()) {
				logger.debug("Got transfer package");
			}
			readFurtherPackages = handleTransfer(packageHeader, packageSequence);
		} else {	
			final ByteBuffer encodedPackage = readFullPackage(packageHeader);
			readFurtherPackages = handleBufferedPackage(encodedPackage, packageSequence, packageType);
		}
		
		if(readFurtherPackages == false) {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		}	
	}

	/**
	 * Handle a buffered package
	 * @param encodedPackage
	 * @param packageSequence
	 * @param packageType
	 * @return
	 * @throws IOException
	 * @throws PackageEncodeError 
	 */
	protected boolean handleBufferedPackage(final ByteBuffer encodedPackage, final short packageSequence, final short packageType) throws PackageEncodeError {
				
		switch (packageType) {
			case NetworkConst.REQUEST_TYPE_HELLO:
				logger.info("Handskaking with: " + clientSocket.getInetAddress());
				return runHandshake(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_COMPRESSION:
				return handleCompression(encodedPackage, packageSequence);
		
			case NetworkConst.REQUEST_TYPE_DISCONNECT:
				logger.info("Got disconnect package, preparing for connection close: "  + clientSocket.getInetAddress());
				return handleDisconnect(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_DELETE_TABLE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got delete table package");
				}
				return handleDeleteTable(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_DELETE_TUPLE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got delete tuple package");
				}
				return handleDeleteTuple(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_LIST_TABLES:
				if(logger.isDebugEnabled()) {
					logger.debug("Got list tables request");
				}
				return handleListTables(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_INSERT_TUPLE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got insert tuple request");
				}
				return handleInsertTuple(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_QUERY:
				if(logger.isDebugEnabled()) {
					logger.debug("Got query package");
				}
				return handleQuery(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_CREATE_DISTRIBUTION_GROUP:
				if(logger.isDebugEnabled()) {
					logger.debug("Got create distribution group package");
				}
				return handleCreateDistributionGroup(encodedPackage, packageSequence);
		
			case NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP:
				if(logger.isDebugEnabled()) {
					logger.debug("Got delete distribution group package");
				}
				return handleDeleteDistributionGroup(encodedPackage, packageSequence);
			
			case NetworkConst.REQUEST_TYPE_KEEP_ALIVE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got keep alive package");
				}
				return handleKeepAlivePackage(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_NEXT_PAGE:
				if(logger.isDebugEnabled()) {
					logger.debug("Got next page package");
				}
				return handleNextPagePackage(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_CANCEL_QUERY:
				if(logger.isDebugEnabled()) {
					logger.debug("Got cancel query package");
				}
				return handleCancelQueryPackage(encodedPackage, packageSequence);

			default:
				logger.warn("Got unknown package type, closing connection: " + packageType);
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				return false;
		}
	}

	/**
	 * Send a new result tuple to the client (compressed or uncompressed)
	 * @param packageSequence
	 * @param requestTable
	 * @param tuple
	 */
	protected void writeResultTuple(final short packageSequence, final SSTableName requestTable, final Tuple tuple) {
		final TupleResponse responsePackage = new TupleResponse(packageSequence, requestTable.getFullname(), tuple);
		
		if(connectionCapabilities.hasGZipCompression()) {
			final CompressionEnvelopeResponse compressionEnvelopeResponse = new CompressionEnvelopeResponse(responsePackage, NetworkConst.COMPRESSION_TYPE_GZIP);
			writeResultPackage(compressionEnvelopeResponse);
		} else {
			writeResultPackage(responsePackage);
		}
	}



}
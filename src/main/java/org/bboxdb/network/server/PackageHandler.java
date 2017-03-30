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
package org.bboxdb.network.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.DistributionRegionState;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.PackageEncodeException;
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
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.packages.response.ListTablesResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.routing.PackageRouter;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.StorageRegistry;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.queryplan.BoundingBoxAndTimeQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.BoundingBoxQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.NewerAsTimeQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.sstable.SSTableManager;
import org.bboxdb.util.ExceptionSafeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageHandler implements Closeable {

	/**
	 * The client connection handler
	 */
	protected final ClientConnectionHandler clientConnectionHandler;
	
	/**
	 * The open query iterators, i.e., the queries that are not finished and waiting
	 * to send the next page
	 */
	protected final Map<Short, ClientQuery> activeQueries;
	
	/**
	 * The thread pool
	 */
	protected final ThreadPoolExecutor threadPool;
	
	/**
	 * The package router
	 */
	protected final PackageRouter packageRouter;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PackageHandler.class);
	
	/**
	 * Number of pending requests
	 */
	public final static int PENDING_REQUESTS = 25;


	public PackageHandler(final ClientConnectionHandler clientConnectionHandler) {
		this.clientConnectionHandler = clientConnectionHandler;
		
		// The active queries
		activeQueries = new HashMap<Short, ClientQuery>();
		
		// Create a thread pool that blocks after submitting more than PENDING_REQUESTS
		final BlockingQueue<Runnable> linkedBlockingDeque = new LinkedBlockingDeque<Runnable>(PENDING_REQUESTS);
		threadPool = new ThreadPoolExecutor(1, PENDING_REQUESTS/2, 30, TimeUnit.SECONDS, 
				linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy());
		
		// The package router
		packageRouter = new PackageRouter(threadPool, clientConnectionHandler);
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
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_NOT_FOUND));
			} else {
				final ClientQuery clientQuery = activeQueries.remove(packageSequence);
				clientQuery.close();
				clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
			}
		} catch (PackageEncodeException e) {
			logger.warn("Error getting next page for a query", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
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
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY));
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
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while delete distribution group", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}
		
		return true;
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
			final QueryPlan queryPlan = new BoundingBoxQueryPlan(queryRequest.getBoundingBox());
			
			final ClientQuery clientQuery = new ClientQuery(queryPlan, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, requestTable);
			
			activeQueries.put(packageSequence, clientQuery);
			sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PackageEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
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
	
			final QueryPlan queryPlan = new BoundingBoxAndTimeQueryPlan(queryRequest.getBoundingBox(), 
					queryRequest.getTimestamp());
	
			final ClientQuery clientQuery = new ClientQuery(queryPlan, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, requestTable);
			
			activeQueries.put(packageSequence, clientQuery);
			sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PackageEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}
	}
	
	/**
	 * Send next results for the given query
	 * @param packageSequence
	 * @param  
	 */
	public void sendNextResultsForQuery(final short packageSequence, final short querySequence) {
			
		if(! activeQueries.containsKey(querySequence)) {
			logger.error("Unable to resume query {} - package {} - not found", querySequence, packageSequence);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_NOT_FOUND));
			return;
		}
		
		final Runnable queryRunable = new ExceptionSafeThread() {

			@Override
			protected void runThread() {
				final ClientQuery clientQuery = activeQueries.get(querySequence);
				clientQuery.fetchAndSendNextTuples(packageSequence);
				
				if(clientQuery.isQueryDone()) {
					logger.debug("Query {} is done, closing and removing iterator", querySequence);
					clientQuery.close();
					activeQueries.remove(querySequence);
				}
			}
			
			@Override
			protected void afterExceptionHook() {
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));
			}
		};

		// Submit the runnable to our pool
		if(threadPool.isTerminating()) {
			logger.warn("Thread pool is shutting down, don't execute query: {}", querySequence);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));
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
			final QueryPlan queryPlan = new NewerAsTimeQueryPlan(queryRequest.getTimestamp());
			
			final ClientQuery clientQuery = new ClientQuery(queryPlan, queryRequest.isPagingEnabled(), 
					queryRequest.getTuplesPerPage(), clientConnectionHandler, packageSequence, requestTable);
			
			activeQueries.put(packageSequence, clientQuery);
			sendNextResultsForQuery(packageSequence, packageSequence);
		} catch (PackageEncodeException e) {
			logger.warn("Got exception while decoding package", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
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
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}
			
			final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage);
			
			// Send the call to the storage manager
			final Tuple tuple = insertTupleRequest.getTuple();			
			final SSTableName requestTable = insertTupleRequest.getTable();
			
			final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final BoundingBox boundingBox = insertTupleRequest.getTuple().getBoundingBox();
			final Collection<SSTableName> localTables = regionIdMapper.getLocalTablesForRegion(boundingBox, requestTable);

			for(final SSTableName ssTableName : localTables) {	
				insertTupleNE(tuple, ssTableName);
			}

			packageRouter.performInsertPackageRoutingAsync(packageSequence, insertTupleRequest, boundingBox);
			
		} catch (Exception e) {
			logger.warn("Error while insert tuple", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}
		
		return true;
	}

	/**
	 * Insert an tuple with proper exception handling
	 * @param tuple
	 * @param ssTableName
	 * @return
	 */
	protected boolean insertTupleNE(final Tuple tuple, final SSTableName ssTableName) {
		
		SSTableManager storageManager = null;
		
		try {
			storageManager = StorageRegistry.getSSTableManager(ssTableName);
		} catch (StorageManagerException e) {
			logger.warn("Got an exception while inserting", e);
			return false;
		}

		try {
			if(! storageManager.isReady()) {
				return false;
			}
			
			storageManager.put(tuple);
			return true;
			
		} catch (StorageManagerException e) {
			if(storageManager.isReady()) {
				logger.warn("Got an exception while inserting", e);
			} else {
				logger.debug("Got an exception while inserting", e);
			}
		}
		
		return false;
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
		clientConnectionHandler.writeResultPackage(listTablesResponse);
		
		return true;
	}

	/**
	 * Handle delete tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 * @throws PackageEncodeException 
	 */
	protected boolean handleDeleteTuple(final ByteBuffer encodedPackage, final short packageSequence) throws PackageEncodeException {

		try {
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				final ErrorResponse errorResponse = new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY);
				clientConnectionHandler.writeResultPackage(errorResponse);
				return true;
			}
			
			final DeleteTupleRequest deleteTupleRequest = DeleteTupleRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = deleteTupleRequest.getTable();

			// Send the call to the storage manager
			final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final Collection<SSTableName> localTables = regionIdMapper.getAllLocalTables(requestTable);

			for(final SSTableName ssTableName : localTables) {
				final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
				storageManager.delete(deleteTupleRequest.getKey(), deleteTupleRequest.getTimestamp());
			}
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		} 

		return true;
	}	
	
	/**
	 * Handle the disconnect request
	 * @param encodedPackage
	 * @return
	 */
	protected boolean handleDisconnect(final ByteBuffer encodedPackage, final short packageSequence) {
		clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		return false;
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
				
			final ErrorResponse errorResponse = new ErrorResponse(packageSequence, 
					ErrorMessages.ERROR_UNSUPPORTED_PACKAGE_TYPE);
			clientConnectionHandler.writeResultPackage(errorResponse);
				
			return true;
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
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY);
				clientConnectionHandler.writeResultPackage(responsePackage);
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
			
			distributionAdapter.allocateSystemsToNewRegion(region);
			
			// Let the data settle down
			Thread.sleep(5000);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(region, DistributionRegionState.ACTIVE);
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (Exception e) {
			logger.warn("Error while create distribution group", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
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
		final SuccessResponse responsePackage = new SuccessResponse(packageSequence);
		clientConnectionHandler.writeResultPackage(responsePackage);
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
			
			// Send tuples as result for original query
			sendNextResultsForQuery(packageSequence, nextPagePackage.getQuerySequence());

		} catch (PackageEncodeException e) {
			logger.warn("Error getting next page for a query", e);
			final ErrorResponse errorResponse = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(errorResponse);	
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
			clientConnectionHandler.setConnectionCapabilities(heloRequest.getPeerCapabilities());

			final HelloResponse responsePackage = new HelloResponse(packageSequence, NetworkConst.PROTOCOL_VERSION, clientConnectionHandler.getConnectionCapabilities());
			clientConnectionHandler.writeResultPackage(responsePackage);

			clientConnectionHandler.connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
			return true;
		} catch(Exception e) {
			logger.warn("Error while reading network package", e);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
			clientConnectionHandler.writeResultPackage(responsePackage);
			return false;
		}
	}
	
	/**
	 * Handle compressed packages. Uncompress envelope and handle package
	 * @param encodedPackage
	 * @param packageSequence
	 * @return
	 * @throws PackageEncodeException 
	 */
	protected boolean handleCompression(final ByteBuffer encodedPackage, final short packageSequence) throws PackageEncodeException {

		try {
			final InputStream compressedDataStream = CompressionEnvelopeRequest.decodePackage(encodedPackage);
						
			while(compressedDataStream.available() > 0) {
				clientConnectionHandler.handleNextPackage(compressedDataStream);
			}
			
		} catch (IOException e) {
			logger.error("Got an exception while handling compression", e);
		}
		
		return true;
	}
	
	/**
	 * Handle the delete table call
	 * @param packageSequence 
	 * @return
	 * @throws PackageEncodeException 
	 */
	protected boolean handleDeleteTable(final ByteBuffer encodedPackage, final short packageSequence) throws PackageEncodeException {
		
		try {
			if(clientConnectionHandler.getNetworkConnectionServiceState().isReadonly()) {
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_INSTANCE_IS_READ_ONLY);
				clientConnectionHandler.writeResultPackage(responsePackage);
				return true;
			}
			
			final DeleteTableRequest deletePackage = DeleteTableRequest.decodeTuple(encodedPackage);
			final SSTableName requestTable = deletePackage.getTable();
			logger.info("Got delete call for table: " + requestTable);
			
			// Send the call to the storage manager
			final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
			final Collection<SSTableName> localTables = regionIdMapper.getAllLocalTables(requestTable);
			
			for(final SSTableName ssTableName : localTables) {
				StorageRegistry.deleteTable(ssTableName);	
			}
			
			clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
		} catch (StorageManagerException e) {
			logger.warn("Error while delete tuple", e);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION));	
		}
		
		return true;
	}

	/**
	 * Handle a key query
	 * @param encodedPackage
	 * @param packageSequence
	 */
	protected void handleKeyQuery(final ByteBuffer encodedPackage, final short packageSequence) {

		final Runnable queryRunable = new ExceptionSafeThread() {

			@Override
			public void runThread() throws Exception {
				final QueryKeyRequest queryKeyRequest = QueryKeyRequest.decodeTuple(encodedPackage);
				final SSTableName requestTable = queryKeyRequest.getTable();
				
				// Send the call to the storage manager
				final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(requestTable.getDistributionGroupObject());
				final Collection<SSTableName> localTables = regionIdMapper.getAllLocalTables(requestTable);
				
				for(final SSTableName ssTableName : localTables) {
					final SSTableManager storageManager = StorageRegistry.getSSTableManager(ssTableName);
					final Tuple tuple = storageManager.get(queryKeyRequest.getKey());
					
					if(tuple != null) {
						clientConnectionHandler.writeResultTuple(packageSequence, requestTable, tuple);
						return;
					}
				}

				clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
				return;
			}			
			
			@Override
			protected void afterExceptionHook() {
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_EXCEPTION);
				clientConnectionHandler.writeResultPackage(responsePackage);	
			}
		};

		// Submit the runnable to our pool
		if(threadPool.isTerminating()) {
			logger.warn("Thread pool is shutting down, don't execute query: {}", packageSequence);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_SHUTDOWN);
			clientConnectionHandler.writeResultPackage(responsePackage);
		} else {
			threadPool.submit(queryRunable);
		}
	}
	

	@Override
	public void close() throws IOException {
		
		threadPool.shutdown();
		
		// Close active query iterators
		for(final ClientQuery clientQuery : activeQueries.values()) {
			clientQuery.close();
		}
		
		activeQueries.clear();		
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
			final short packageType) throws PackageEncodeException {
				
		switch (packageType) {
			case NetworkConst.REQUEST_TYPE_HELLO:
				logger.info("Handshaking with: " 
						+ clientConnectionHandler.clientSocket.getInetAddress());
				return runHandshake(encodedPackage, packageSequence);
				
			case NetworkConst.REQUEST_TYPE_COMPRESSION:
				return handleCompression(encodedPackage, packageSequence);
		
			case NetworkConst.REQUEST_TYPE_DISCONNECT:
				logger.info("Got disconnect package, preparing for connection close: "  
						+ clientConnectionHandler.clientSocket.getInetAddress());
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
				clientConnectionHandler.connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				return false;
		}
	}

}

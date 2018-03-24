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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.FutureHelper;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.request.CancelQueryRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.CreateTableRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxContinuousRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxTimeRequest;
import org.bboxdb.network.packages.request.QueryInsertTimeRequest;
import org.bboxdb.network.packages.request.QueryJoinRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryVersionTimeRequest;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBClient implements BBoxDB {

	/**
	 * The Connection
	 */
	private BBoxDBConnection connection;
	
	/**
	 * Is the paging for queries enabled?
	 */
	private boolean pagingEnabled;

	/**
	 * The amount of tuples per page
	 */
	private short tuplesPerPage;
		
	/**
	 * The tuple store manager registry (used for gossip)
	 */
	private TupleStoreManagerRegistry tupleStoreManagerRegistry;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBClient.class);

	public BBoxDBClient(final BBoxDBConnection connection) {
		this.connection = Objects.requireNonNull(connection);
		this.pagingEnabled = true;
		this.tuplesPerPage = 50;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#createTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture createTable(final String table, final TupleStoreConfiguration configuration) throws BBoxDBException {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("createTable called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final short nextSequenceNumber = connection.getNextSequenceNumber();
		final CreateTableRequest requestPackage = new CreateTableRequest(nextSequenceNumber, table, configuration);
		
		sendPackageToServer(clientOperationFuture, requestPackage, false);
		
		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTable(final String table) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("deleteTable called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final short nextSequenceNumber = connection.getNextSequenceNumber();
		final DeleteTableRequest requestPackage = new DeleteTableRequest(nextSequenceNumber, table);
		
		sendPackageToServer(clientOperationFuture, requestPackage, false);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {
		
		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("insertTuple called, but connection not ready: " + this);
		}

		final Supplier<RoutingHeader> routingHeader = () -> RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, tuple.getBoundingBox(), false, connection.getServerAddress());

		return insertTuple(table, tuple, routingHeader);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple, 
			final Supplier<RoutingHeader> routingHeaderSupplier) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("insertTuple called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final TupleStoreName ssTableName = new TupleStoreName(table);
		final short sequenceNumber = connection.getNextSequenceNumber();

		final InsertTupleRequest requestPackage = new InsertTupleRequest(
				sequenceNumber, 
				routingHeaderSupplier, 
				ssTableName, 
				tuple);

		sendPackageToServer(clientOperationFuture, requestPackage, false);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) {
		final Supplier<RoutingHeader> routingHeader = () -> RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, BoundingBox.FULL_SPACE, true, connection.getServerAddress());
		
		return insertTuple(table, new DeletedTuple(key, timestamp), routingHeader);
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
	 * @see org.bboxdb.network.client.BBoxDB#createDistributionGroup(java.lang.String, short)
	 */
	@Override
	public EmptyResultFuture createDistributionGroup(final String distributionGroup, 
			final DistributionGroupConfiguration distributionGroupConfiguration) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("listTables called, but connection not ready: " + this);
		}

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final short nextSequenceNumber = connection.getNextSequenceNumber();

		final CreateDistributionGroupRequest requestPackage = new CreateDistributionGroupRequest(
				nextSequenceNumber, distributionGroup, 
				distributionGroupConfiguration);

		sendPackageToServer(clientOperationFuture, requestPackage, false);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteDistributionGroup(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("delete distribution group called, but connection not ready: " + this);
		}
		
		final short nextSequenceNumber = connection.getNextSequenceNumber();

		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		final DeleteDistributionGroupRequest requestPackage = new DeleteDistributionGroupRequest(
				nextSequenceNumber, distributionGroup);

		sendPackageToServer(clientOperationFuture, requestPackage, false);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryKey(java.lang.String, java.lang.String)
	 */
	@Override
	public TupleListFuture queryKey(final String table, final String key) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryKey called, but connection not ready: " + this, table);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
						table, BoundingBox.FULL_SPACE, true, connection.getServerAddress());
		
		final DuplicateResolver<Tuple> duplicateResolver 
			= TupleStoreConfigurationCache.getInstance().getDuplicateResolverForTupleStore(table);
		
		final TupleListFuture clientOperationFuture = new TupleListFuture(1, duplicateResolver, table);
		final short nextSequenceNumber = connection.getNextSequenceNumber();

		final QueryKeyRequest requestPackage = new QueryKeyRequest(nextSequenceNumber, 
				routingHeaderSupplier, table, key, pagingEnabled, tuplesPerPage);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBox(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryBoundingBox(final String table, final BoundingBox boundingBox) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryBoundingBox called, but connection not ready: " + this, table);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
						table, boundingBox, false, connection.getServerAddress());

		final short nextSequenceNumber = connection.getNextSequenceNumber();

		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver(), table);
		final QueryBoundingBoxRequest requestPackage = new QueryBoundingBoxRequest(nextSequenceNumber, 
				routingHeaderSupplier, table, boundingBox, pagingEnabled, tuplesPerPage);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/**
	 * Execute a continuous bounding box query
	 * 
	 */
	@Override
	public TupleListFuture queryBoundingBoxContinuous(final String table, final BoundingBox boundingBox) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryBoundingBoxContinuous called, but connection not ready: " + this, table);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
						table, boundingBox, false, connection.getServerAddress());
		
				final short nextSequenceNumber = connection.getNextSequenceNumber();
			
		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver(), table);
		final QueryBoundingBoxContinuousRequest requestPackage = new QueryBoundingBoxContinuousRequest(
				nextSequenceNumber, routingHeaderSupplier, table, boundingBox);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBoxAndTime(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryBoundingBoxAndTime(final String table,
			final BoundingBox boundingBox, final long timestamp) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryBoundingBox called, but connection not ready: " + this, table);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
						table,boundingBox, false, connection.getServerAddress());
		
		final short nextSequenceNumber = connection.getNextSequenceNumber();

		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver(), table);
		final QueryBoundingBoxTimeRequest requestPackage = new QueryBoundingBoxTimeRequest(nextSequenceNumber, 
				routingHeaderSupplier, table, boundingBox, timestamp, pagingEnabled, tuplesPerPage);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryVersionTime(final String table, final long timestamp) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryTime called, but connection not ready: " + this, table);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
						table, BoundingBox.FULL_SPACE, true, connection.getServerAddress());

		final short nextSequenceNumber = connection.getNextSequenceNumber();
				
		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver(), table);
		final QueryVersionTimeRequest requestPackage = new QueryVersionTimeRequest(nextSequenceNumber, 
				routingHeaderSupplier, table, timestamp, pagingEnabled, tuplesPerPage);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedTupleListFuture("queryTime called, but connection not ready: " + this, table);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
						table, BoundingBox.FULL_SPACE, true, connection.getServerAddress());

		final short nextSequenceNumber = connection.getNextSequenceNumber();
				
		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver(), table);
		final QueryInsertTimeRequest requestPackage = new QueryInsertTimeRequest(nextSequenceNumber, 
				routingHeaderSupplier, table, timestamp, pagingEnabled, tuplesPerPage);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryJoin
	 */
	@Override
	public JoinedTupleListFuture queryJoin(final List<String> tableNames, final BoundingBox boundingBox) {
		
		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedJoinedTupleListFuture("queryTime called, but connection not ready: " + this);
		}

		final Supplier<RoutingHeader> routingHeaderSupplier = () 
				-> RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
					tableNames.get(0), boundingBox, true, connection.getServerAddress());

		final JoinedTupleListFuture clientOperationFuture = new JoinedTupleListFuture(1);
		
		final List<TupleStoreName> tupleStoreNames = tableNames
				.stream()
				.map(t -> new TupleStoreName(t))
				.collect(Collectors.toList());
		
		final short nextSequenceNumber = connection.getNextSequenceNumber();
		
		final QueryJoinRequest requestPackage = new QueryJoinRequest(nextSequenceNumber, 
				routingHeaderSupplier, tupleStoreNames, boundingBox, pagingEnabled, tuplesPerPage);
		
		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/**
	 * Send a keep alive package to the server, to keep the TCP connection open.
	 * @return
	 */
	public EmptyResultFuture sendKeepAlivePackage() {
		return sendKeepAlivePackage("", new ArrayList<>());
	}
	
	/**
	 * Send a keep alive package with some gossip
	 * @param tablename
	 * @param tuples
	 * @return
	 */
	public EmptyResultFuture sendKeepAlivePackage(final String tablename, final List<Tuple> tuples) {

		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("sendKeepAlivePackage called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		
		final short nextSequenceNumber = connection.getNextSequenceNumber();
		
		final KeepAliveRequest requestPackage 
			= new KeepAliveRequest(nextSequenceNumber, tablename, tuples);
		
		sendPackageToServer(clientOperationFuture, requestPackage, false);

		return clientOperationFuture;
	}

	/**
	 * Get the next page for a given query
	 * @param queryPackageId
	 * @return
	 */
	public OperationFuture getNextPage(final short queryPackageId) {

		if(! connection.getResultBuffer().containsKey(queryPackageId)) {
			final String errorMessage = "Query package " + queryPackageId 
					+ " not found in the result buffer";

			logger.error(errorMessage);
			return FutureHelper.getFailedTupleListFuture(errorMessage, "");
		}

		final TupleListFuture clientOperationFuture = new TupleListFuture(1, new DoNothingDuplicateResolver(), "");
		
		final short nextSequenceNumber = connection.getNextSequenceNumber();
		
		final NextPageRequest requestPackage = new NextPageRequest(
				nextSequenceNumber, queryPackageId);

		sendPackageToServer(clientOperationFuture, requestPackage, true);

		return clientOperationFuture;
	}

	/**
	 * Send the package to the server
	 * 
	 * @param clientOperationFuture
	 * @param requestPackage
	 */
	private void sendPackageToServer(final OperationFuture clientOperationFuture,
			final NetworkRequestPackage requestPackage, final boolean flush) {
		
		connection.registerPackageCallback(requestPackage, clientOperationFuture);
		connection.sendPackageToServer(requestPackage, clientOperationFuture);

		if(flush) {
			connection.flushPendingCompressionPackages();
		}
	}

	/**
	 * Cancel the given query on the server
	 * @param queryPackageId
	 * @return
	 */
	public EmptyResultFuture cancelQuery(final short queryPackageId) {
		
		if(! connection.getConnectionState().isInRunningState()) {
			return FutureHelper.getFailedEmptyResultFuture("cancelQuery called, but connection not ready: " + this);
		}
		
		final EmptyResultFuture clientOperationFuture = new EmptyResultFuture(1);
		
		final short nextSequenceNumber = connection.getNextSequenceNumber();

		final CancelQueryRequest requestPackage = new CancelQueryRequest(nextSequenceNumber, queryPackageId);

		sendPackageToServer(clientOperationFuture, requestPackage, false);

		return clientOperationFuture;
	}

	@Override
	public String toString() {
		return "BBoxDBClient [connection=" + connection + "]";
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

	@Override
	public boolean connect() {
		return connection.connect();
	}

	@Override
	public void disconnect() {
		connection.disconnect();
	}

	@Override
	public boolean isConnected() {
		return connection.isConnected();
	}
	
	/**
	 * The tuple store manager registry
	 * @return
	 */
	public TupleStoreManagerRegistry getTupleStoreManagerRegistry() {
		return tupleStoreManagerRegistry;
	}

	/**
	 * The tuple store manager registry
	 * @param tupleStoreManagerRegistry
	 */
	public void setTupleStoreManagerRegistry(final TupleStoreManagerRegistry tupleStoreManagerRegistry) {
		this.tupleStoreManagerRegistry = tupleStoreManagerRegistry;
	}

	@Override
	public int getInFlightCalls() {
		return connection.getInFlightCalls();
	}
}

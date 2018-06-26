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
import java.util.stream.Collectors;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.FutureRetryPolicy;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.NetworkOperationFuture;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.request.CancelRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.CreateTableRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.LockTupleRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleContinuousRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleTimeRequest;
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

public class BBoxDBClient implements BBoxDB {

	/**
	 * The Connection
	 */
	private final BBoxDBConnection connection;
	
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

	public BBoxDBClient(final BBoxDBConnection connection) {
		this.connection = Objects.requireNonNull(connection);
		this.pagingEnabled = true;
		this.tuplesPerPage = 50;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#createTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture createTable(final String table, final TupleStoreConfiguration configuration) 
			throws BBoxDBException {

		final NetworkOperationFuture future = getCreateTableFugure(table, configuration);
		
		return new EmptyResultFuture(future);
	}

	/**
	 * @param table
	 * @param configuration
	 * @return
	 */
	public NetworkOperationFuture getCreateTableFugure(final String table,
			final TupleStoreConfiguration configuration) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new CreateTableRequest(nextSequenceNumber, table, configuration);
		});		
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTable(final String table) {
		return new EmptyResultFuture(getDeleteTableFuture(table));
	}

	/**
	 * @param table
	 * @return
	 */
	public NetworkOperationFuture getDeleteTableFuture(final String table) {
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new DeleteTableRequest(nextSequenceNumber, table);
		});
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, tuple.getBoundingBox(), false, connection.getServerAddress());

		return insertTuple(table, tuple, routingHeader);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple, 
			final RoutingHeader routingHeader) {

		final NetworkOperationFuture future = getInsertTupleFuture(table, tuple, routingHeader);
		
		return new EmptyResultFuture(future);
	}

	@Override
	public EmptyResultFuture lockTuple(final String table, final Tuple tuple, 
			final boolean deleteOnTimeout) throws BBoxDBException {
		
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());
	
		final NetworkOperationFuture future = createLockTupleFuture(table, tuple, deleteOnTimeout, 
				routingHeader);

		// When version locking fails, try again with another version
		return new EmptyResultFuture(future, FutureRetryPolicy.RETRY_POLICY_NONE);
	}
	
	/**
	 * Create the lock tuple future
	 * @param table
	 * @param deleteOnTimeout 
	 * @param key
	 * @param version
	 * @param routingHeader
	 * @return
	 */
	public NetworkOperationFuture createLockTupleFuture(final String table, final Tuple tuple, 
			final boolean deleteOnTimeout, final RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			final String key = tuple.getKey();
			final long timestamp = tuple.getVersionTimestamp();
			
			return new LockTupleRequest(
					nextSequenceNumber, routingHeader, table, key, timestamp, deleteOnTimeout); 
		});
	}

	/**
	 * @param table
	 * @param tuple
	 * @param routingHeader
	 * @return
	 */
	public NetworkOperationFuture getInsertTupleFuture(final String table, final Tuple tuple,
			final RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final TupleStoreName ssTableName = new TupleStoreName(table);
			final short sequenceNumber = connection.getNextSequenceNumber();

			return new InsertTupleRequest(sequenceNumber, routingHeader, ssTableName, tuple);
		});
		
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp, 
			final Hyperrectangle boundingBox) {
		
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, boundingBox, true, connection.getServerAddress());
		
		return insertTuple(table, new DeletedTuple(key, timestamp), routingHeader);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) {
		return deleteTuple(table, key, timestamp, Hyperrectangle.FULL_SPACE);
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
		
		final NetworkOperationFuture future = getCreateDistributionGroupFuture(distributionGroup,
				distributionGroupConfiguration);

		return new EmptyResultFuture(future);
	}

	/**
	 * @param distributionGroup
	 * @param distributionGroupConfiguration
	 * @return
	 */
	public NetworkOperationFuture getCreateDistributionGroupFuture(final String distributionGroup,
			final DistributionGroupConfiguration distributionGroupConfiguration) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new CreateDistributionGroupRequest(
					nextSequenceNumber, distributionGroup, 
					distributionGroupConfiguration);
		});
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteDistributionGroup(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) {
		final NetworkOperationFuture future = getDeleteDistributionGroupFuture(distributionGroup);
		return new EmptyResultFuture(future);
	}

	/**
	 * @param distributionGroup
	 * @return
	 */
	public NetworkOperationFuture getDeleteDistributionGroupFuture(final String distributionGroup) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new DeleteDistributionGroupRequest(nextSequenceNumber, distributionGroup);
		});
		
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryKey(java.lang.String, java.lang.String)
	 */
	@Override
	public TupleListFuture queryKey(final String table, final String key) {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());
		
		final NetworkOperationFuture future = getQueryKeyFuture(table, key, routingHeader);
		
		final DuplicateResolver<Tuple> duplicateResolver 
			= TupleStoreConfigurationCache.getInstance().getDuplicateResolverForTupleStore(table);

		return new TupleListFuture(future, duplicateResolver, table);
	}

	/**
	 * @param table
	 * @param key
	 * @return
	 */
	public NetworkOperationFuture getQueryKeyFuture(final String table, final String key, 
			final RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
	
			return new QueryKeyRequest(nextSequenceNumber, 
					routingHeader, table, key, pagingEnabled, tuplesPerPage);
		});
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBox(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryRectangle(final String table, final Hyperrectangle boundingBox) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, boundingBox, false, connection.getServerAddress());
		final NetworkOperationFuture future = getQueryBoundingBoxFuture(table, boundingBox, routingHeader);
		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * 
	 * @param table
	 * @param boundingBox
	 * @param routingHeader 
	 * @return
	 */
	public NetworkOperationFuture getQueryBoundingBoxFuture(final String table, 
			final Hyperrectangle boundingBox, RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			
			return new QueryHyperrectangleRequest(nextSequenceNumber, 
					routingHeader, table, boundingBox, pagingEnabled, tuplesPerPage);
		});
	}

	/**
	 * Execute a continuous bounding box query
	 * 
	 */
	@Override
	public TupleListFuture queryRectangleContinuous(final String table, final Hyperrectangle boundingBox) {
		final NetworkOperationFuture future = getQueryBoundingBoxContinousFuture(table, boundingBox);
		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * @param table
	 * @param boundingBox
	 * @return
	 */
	public NetworkOperationFuture getQueryBoundingBoxContinousFuture(final String table,
			final Hyperrectangle boundingBox) {
		
		return new NetworkOperationFuture(connection, () -> {
			final RoutingHeader routingHeaderSupplier = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
					table, boundingBox, false, connection.getServerAddress());

			final short nextSequenceNumber = connection.getNextSequenceNumber();
			
			return new QueryHyperrectangleContinuousRequest(
					nextSequenceNumber, routingHeaderSupplier, table, boundingBox);
		});
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBoxAndTime(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryRectangleAndTime(final String table,
			final Hyperrectangle boundingBox, final long timestamp) {
		
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, boundingBox, false, connection.getServerAddress());
		
		final NetworkOperationFuture future = getBoundingBoxAndTimeFuture(table, boundingBox, 
				timestamp, routingHeader);
		
		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * @param table
	 * @param boundingBox
	 * @param timestamp
	 * @param routingHeader 
	 * @return
	 */
	public NetworkOperationFuture getBoundingBoxAndTimeFuture(final String table, final Hyperrectangle boundingBox,
			final long timestamp, RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			
			return new QueryHyperrectangleTimeRequest(nextSequenceNumber, 
					routingHeader, table, boundingBox, timestamp, pagingEnabled, tuplesPerPage);
		});
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryVersionTime(final String table, final long timestamp) {
		final RoutingHeader routingHeader =  RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());

		final NetworkOperationFuture future = getVersionTimeFuture(table, timestamp, routingHeader);
		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * @param table
	 * @param timestamp
	 * @param routingHeader 
	 * @return
	 */
	public NetworkOperationFuture getVersionTimeFuture(final String table, final long timestamp, 
			final RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
					
			return new QueryVersionTimeRequest(nextSequenceNumber, 
					routingHeader, table, timestamp, pagingEnabled, tuplesPerPage);
		});
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());
		final NetworkOperationFuture future = getInsertedTimeFuture(table, timestamp, routingHeader);
		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * @param table
	 * @param timestamp
	 * @param routingHeader2 
	 * @return
	 */
	public NetworkOperationFuture getInsertedTimeFuture(final String table, final long timestamp, 
			final RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new QueryInsertTimeRequest(nextSequenceNumber, 
					routingHeader, table, timestamp, pagingEnabled, tuplesPerPage);
		});
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryJoin
	 */
	@Override
	public JoinedTupleListFuture queryJoin(final List<String> tableNames, final Hyperrectangle boundingBox) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				tableNames.get(0), boundingBox, true, connection.getServerAddress());
		
		final NetworkOperationFuture future = getJoinFuture(tableNames, boundingBox, routingHeader);
		return new JoinedTupleListFuture(future);
	}

	/**
	 * @param tableNames
	 * @param boundingBox
	 * @param routingHeader2 
	 * @return
	 */
	public NetworkOperationFuture getJoinFuture(final List<String> tableNames, final Hyperrectangle boundingBox, 
			final RoutingHeader routingHeader) {
		
		return new NetworkOperationFuture(connection, () -> {

			final List<TupleStoreName> tupleStoreNames = tableNames
					.stream()
					.map(t -> new TupleStoreName(t))
					.collect(Collectors.toList());
			
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			
			return new QueryJoinRequest(nextSequenceNumber, 
					routingHeader, tupleStoreNames, boundingBox, pagingEnabled, tuplesPerPage);
		});
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
		final NetworkOperationFuture future = getKeepAliveFuture(tablename, tuples);
		final EmptyResultFuture resultFuture = new EmptyResultFuture(future);
		
		// Unsuccesfull means only we have to send gossip data
		resultFuture.setRetryPolicy(FutureRetryPolicy.RETRY_POLICY_NONE);
		
		return resultFuture;
	}

	/**
	 * @param tablename
	 * @param tuples
	 * @return
	 */
	public NetworkOperationFuture getKeepAliveFuture(final String tablename, final List<Tuple> tuples) {
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new KeepAliveRequest(nextSequenceNumber, tablename, tuples);
		});
	}

	/**
	 * Get the next page for a given query
	 * @param queryPackageId
	 * @return
	 */
	public OperationFuture getNextPage(final short queryPackageId) {
		
		final NetworkOperationFuture future = new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			
			return new NextPageRequest(nextSequenceNumber, queryPackageId);
		});

		return new TupleListFuture(future, new DoNothingDuplicateResolver(), "");
	}

	/**
	 * Cancel the given request on the server
	 * @param queryPackageId
	 * @return
	 */
	public EmptyResultFuture cancelRequest(final short queryPackageId) {
		final NetworkOperationFuture future = getCancelQueryFuture(queryPackageId);
		return new EmptyResultFuture(future, FutureRetryPolicy.RETRY_POLICY_NONE);
	}

	/**
	 * @param queryPackageId
	 * @return
	 */
	public NetworkOperationFuture getCancelQueryFuture(final short queryPackageId) {
		return new NetworkOperationFuture(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new CancelRequest(nextSequenceNumber, queryPackageId);
		});
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
	public void close() {
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

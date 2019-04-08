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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.AbstractListFuture;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.FutureRetryPolicy;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.NetworkOperationFuture;
import org.bboxdb.network.client.future.NetworkOperationFutureImpl;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.request.CancelRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.CreateTableRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.InsertOption;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.LockTupleRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryContinuousRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleTimeRequest;
import org.bboxdb.network.packages.request.QueryInsertTimeRequest;
import org.bboxdb.network.packages.request.QueryJoinRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryVersionTimeRequest;
import org.bboxdb.network.query.ContinuousQueryPlan;
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

		return new EmptyResultFuture(() -> Arrays.asList(future));
	}

	/**
	 * @param table
	 * @param configuration
	 * @return
	 */
	public NetworkOperationFuture getCreateTableFugure(final String table,
			final TupleStoreConfiguration configuration) {

		return new NetworkOperationFutureImpl(connection, () -> {
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
	public Supplier<List<NetworkOperationFuture>> getDeleteTableFuture(final String table) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new DeleteTableRequest(nextSequenceNumber, table);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, tuple.getBoundingBox(), false, connection.getServerAddress());

		return insertTuple(table, tuple, routingHeader, EnumSet.noneOf(InsertOption.class));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple,
			final RoutingHeader routingHeader, final EnumSet<InsertOption> insertOptions) {

		final Supplier<List<NetworkOperationFuture>> future = 
				getInsertTupleFuture(table, tuple, routingHeader, insertOptions);

		return new EmptyResultFuture(future);
	}

	@Override
	public EmptyResultFuture lockTuple(final String table, final Tuple tuple,
			final boolean deleteOnTimeout) throws BBoxDBException {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future =
				createLockTupleFuture(table, tuple, deleteOnTimeout, routingHeader);

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
	public Supplier<List<NetworkOperationFuture>> createLockTupleFuture(final String table, final Tuple tuple,
			final boolean deleteOnTimeout, final RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			final String key = tuple.getKey();
			final long timestamp = tuple.getVersionTimestamp();

			return new LockTupleRequest(
					nextSequenceNumber, routingHeader, table, key, timestamp, deleteOnTimeout);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/**
	 * @param table
	 * @param tuple
	 * @param routingHeader
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getInsertTupleFuture(final String table, final Tuple tuple,
			final RoutingHeader routingHeader, final EnumSet<InsertOption> insertOptions) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final TupleStoreName ssTableName = new TupleStoreName(table);
			final short sequenceNumber = connection.getNextSequenceNumber();

			return new InsertTupleRequest(sequenceNumber, routingHeader, ssTableName, tuple, insertOptions);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTuple(java.lang.String, java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp,
			final Hyperrectangle boundingBox) {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, boundingBox, true, connection.getServerAddress());

		return insertTuple(table, new DeletedTuple(key, timestamp), routingHeader, EnumSet.noneOf(InsertOption.class));
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

		final Supplier<List<NetworkOperationFuture>> future
			= getCreateDistributionGroupFuture(distributionGroup, distributionGroupConfiguration);

		return new EmptyResultFuture(future);
	}

	/**
	 * @param distributionGroup
	 * @param distributionGroupConfiguration
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getCreateDistributionGroupFuture(final String distributionGroup,
			final DistributionGroupConfiguration distributionGroupConfiguration) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new CreateDistributionGroupRequest(
					nextSequenceNumber, distributionGroup,
					distributionGroupConfiguration);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteDistributionGroup(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) {
		final Supplier<List<NetworkOperationFuture>> future
			= getDeleteDistributionGroupFuture(distributionGroup);

		return new EmptyResultFuture(future);
	}

	/**
	 * @param distributionGroup
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getDeleteDistributionGroupFuture(final String distributionGroup) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new DeleteDistributionGroupRequest(nextSequenceNumber, distributionGroup);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryKey(java.lang.String, java.lang.String)
	 */
	@Override
	public TupleListFuture queryKey(final String table, final String key) {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future = getQueryKeyFuture(table, key, routingHeader);

		final DuplicateResolver<Tuple> duplicateResolver
			= TupleStoreConfigurationCache.getInstance().getDuplicateResolverForTupleStore(table);

		return new TupleListFuture(future, duplicateResolver, table);
	}

	/**
	 * @param table
	 * @param key
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getQueryKeyFuture(final String table, final String key,
			final RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryKeyRequest(nextSequenceNumber,
					routingHeader, table, key, pagingEnabled, tuplesPerPage);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBox(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryRectangle(final String table, final Hyperrectangle boundingBox) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, boundingBox, false, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future
			= getQueryBoundingBoxFuture(table, boundingBox, routingHeader);

		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 *
	 * @param table
	 * @param boundingBox
	 * @param routingHeader
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getQueryBoundingBoxFuture(final String table,
			final Hyperrectangle boundingBox, RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryHyperrectangleRequest(nextSequenceNumber,
					routingHeader, table, boundingBox, pagingEnabled, tuplesPerPage);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/**
	 * Execute a continuous bounding box query
	 *
	 */
	@Override
	public JoinedTupleListFuture queryContinuous(final ContinuousQueryPlan queryPlan) {
		final Supplier<List<NetworkOperationFuture>> future
			= getQueryBoundingBoxContinousFuture(queryPlan);

		return new JoinedTupleListFuture(future);
	}

	/**
	 * @param table
	 * @param boundingBox
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getQueryBoundingBoxContinousFuture(
			final ContinuousQueryPlan queryPlan) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final RoutingHeader routingHeaderSupplier = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
					queryPlan.getStreamTable(), queryPlan.getQueryRange(), false, connection.getServerAddress());

			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryContinuousRequest(
					nextSequenceNumber, routingHeaderSupplier, queryPlan);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryBoundingBoxAndTime(java.lang.String, org.bboxdb.storage.entity.BoundingBox)
	 */
	@Override
	public TupleListFuture queryRectangleAndTime(final String table,
			final Hyperrectangle boundingBox, final long timestamp) {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, boundingBox, false, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future = getBoundingBoxAndTimeFuture(table, boundingBox,
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
	public Supplier<List<NetworkOperationFuture>> getBoundingBoxAndTimeFuture(final String table, final Hyperrectangle boundingBox,
			final long timestamp, RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryHyperrectangleTimeRequest(nextSequenceNumber,
					routingHeader, table, boundingBox, timestamp, pagingEnabled, tuplesPerPage);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryVersionTime(final String table, final long timestamp) {
		final RoutingHeader routingHeader =  RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future
			= getVersionTimeFuture(table, timestamp, routingHeader);

		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * @param table
	 * @param timestamp
	 * @param routingHeader
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getVersionTimeFuture(final String table, final long timestamp,
			final RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryVersionTimeRequest(nextSequenceNumber,
					routingHeader, table, timestamp, pagingEnabled, tuplesPerPage);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryTime(java.lang.String, long)
	 */
	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future
			= getInsertedTimeFuture(table, timestamp, routingHeader);

		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * @param table
	 * @param timestamp
	 * @param routingHeader2
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getInsertedTimeFuture(final String table, final long timestamp,
			final RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new QueryInsertTimeRequest(nextSequenceNumber,
					routingHeader, table, timestamp, pagingEnabled, tuplesPerPage);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryJoin
	 */
	@Override
	public JoinedTupleListFuture queryJoin(final List<String> tableNames, final Hyperrectangle boundingBox) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				tableNames.get(0), boundingBox, true, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future
			= getJoinFuture(tableNames, boundingBox, routingHeader);

		return new JoinedTupleListFuture(future);
	}

	/**
	 * @param tableNames
	 * @param boundingBox
	 * @param routingHeader2
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getJoinFuture(final List<String> tableNames, final Hyperrectangle boundingBox,
			final RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPackage> packageSupplier = () -> {

			final List<TupleStoreName> tupleStoreNames = tableNames
					.stream()
					.map(t -> new TupleStoreName(t))
					.collect(Collectors.toList());

			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryJoinRequest(nextSequenceNumber,
					routingHeader, tupleStoreNames, boundingBox, pagingEnabled, tuplesPerPage);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
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
		final Supplier<List<NetworkOperationFuture>> future = getKeepAliveFuture(tablename, tuples);
		final EmptyResultFuture resultFuture = new EmptyResultFuture(future);

		// Unsuccessful means only we have to send gossip data
		resultFuture.setRetryPolicy(FutureRetryPolicy.RETRY_POLICY_NONE);

		return resultFuture;
	}

	/**
	 * @param tablename
	 * @param tuples
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getKeepAliveFuture(final String tablename, final List<Tuple> tuples) {
		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new KeepAliveRequest(nextSequenceNumber, tablename, tuples);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/**
	 * Get the next page for a given query
	 * @param queryPackageId
	 * @return
	 */
	public OperationFuture getNextPage(final short queryPackageId) {
		final Supplier<List<NetworkOperationFuture>> future = getNextPageFuture(queryPackageId);

		return new TupleListFuture(future, new DoNothingDuplicateResolver(), "");
	}

	/**
	 * Get the next page future
	 * @param queryPackageId
	 * @return
	 */
	private Supplier<List<NetworkOperationFuture>> getNextPageFuture(final short queryPackageId) {
		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new NextPageRequest(nextSequenceNumber, queryPackageId);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/**
	 * Cancel the given request on the server
	 * @param queryPackageId
	 * @return
	 */
	public EmptyResultFuture cancelRequest(final short queryPackageId) {
		final Supplier<List<NetworkOperationFuture>> future = getCancelQueryFuture(queryPackageId);
		return new EmptyResultFuture(future, FutureRetryPolicy.RETRY_POLICY_NONE);
	}

	/**
	 * @param queryPackageId
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getCancelQueryFuture(final short queryPackageId) {
		final Supplier<NetworkRequestPackage> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new CancelRequest(nextSequenceNumber, queryPackageId);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/**
	 * Cancel the given query
	 */
	public void cancelQuery(final AbstractListFuture<? extends Object> future)
			throws BBoxDBException, InterruptedException {

		BBoxDBClientHelper.cancelQuery(future);
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

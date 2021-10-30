/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.connection.RoutingHeaderHelper;
import org.bboxdb.network.client.future.client.ContinuousQueryServerStateFuture;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.future.client.FutureRetryPolicy;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.client.OperationFuture;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.client.future.network.NetworkOperationFutureImpl;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.bboxdb.network.packets.request.CancelRequest;
import org.bboxdb.network.packets.request.ContinuousQueryStateRequest;
import org.bboxdb.network.packets.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packets.request.CreateTableRequest;
import org.bboxdb.network.packets.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packets.request.DeleteTableRequest;
import org.bboxdb.network.packets.request.InsertTupleRequest;
import org.bboxdb.network.packets.request.KeepAliveRequest;
import org.bboxdb.network.packets.request.LockTupleRequest;
import org.bboxdb.network.packets.request.NextPageRequest;
import org.bboxdb.network.packets.request.QueryContinuousRequest;
import org.bboxdb.network.packets.request.QueryHyperrectangleRequest;
import org.bboxdb.network.packets.request.QueryHyperrectangleTimeRequest;
import org.bboxdb.network.packets.request.QueryInsertTimeRequest;
import org.bboxdb.network.packets.request.QueryJoinRequest;
import org.bboxdb.network.packets.request.QueryKeyRequest;
import org.bboxdb.network.packets.request.QueryVersionTimeRequest;
import org.bboxdb.network.routing.DistributionRegionHandlingFlag;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
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

		final NetworkOperationFuture future = getCreateTableFuture(table, configuration);

		return new EmptyResultFuture(() -> Arrays.asList(future));
	}

	/**
	 * @param table
	 * @param configuration
	 * @return
	 */
	public NetworkOperationFuture getCreateTableFuture(final String table,
			final TupleStoreConfiguration configuration) {

		final NetworkOperationFutureImpl future = new NetworkOperationFutureImpl(connection, () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new CreateTableRequest(nextSequenceNumber, table, configuration);
		});
		
		// Let the operation fail fast
		future.setTotalRetries(NetworkOperationFutureImpl.FAST_FAIL_RETRIES);
		
		return future;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#deleteTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture deleteTable(final String table) {
		final EmptyResultFuture future = new EmptyResultFuture(getDeleteTableSupplier(table));
		
		return future;
	}

	/**
	 * @param table
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getDeleteTableSupplier(final String table) {

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new DeleteTableRequest(nextSequenceNumber, table);
		};

		return () -> {
			final NetworkOperationFutureImpl networkOperationFuture = new NetworkOperationFutureImpl(connection, packageSupplier);
			
			// Let the operation fail fast
			networkOperationFuture.setTotalRetries(NetworkOperationFutureImpl.FAST_FAIL_RETRIES);
			
			return Arrays.asList(networkOperationFuture);
		};
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, tuple.getBoundingBox(), false, connection.getServerAddress(), EnumSet.noneOf(DistributionRegionHandlingFlag.class));

		return insertTuple(table, tuple, routingHeader);
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple, 
			final EnumSet<DistributionRegionHandlingFlag> insertOptions) throws BBoxDBException {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, tuple.getBoundingBox(), false, connection.getServerAddress(), insertOptions);

		return insertTuple(table, tuple, routingHeader);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#insertTuple(java.lang.String, org.bboxdb.storage.entity.Tuple)
	 */
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple,
			final RoutingHeader routingHeader) {

		final Supplier<List<NetworkOperationFuture>> future = 
				getInsertTupleFuture(table, tuple, routingHeader);

		return new EmptyResultFuture(future);
	}

	@Override
	public EmptyResultFuture lockTuple(final String table, final Tuple tuple,
			final boolean deleteOnTimeout) throws BBoxDBException {

		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemWriteNE(
				table, Hyperrectangle.FULL_SPACE, true, connection.getServerAddress(), 
				EnumSet.noneOf(DistributionRegionHandlingFlag.class));

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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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
			final RoutingHeader routingHeader) {

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final TupleStoreName ssTableName = new TupleStoreName(table);
			final short sequenceNumber = connection.getNextSequenceNumber();

			return new InsertTupleRequest(sequenceNumber, routingHeader, ssTableName, tuple);
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
				table, boundingBox, true, connection.getServerAddress(), EnumSet.noneOf(DistributionRegionHandlingFlag.class));

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

		final Supplier<List<NetworkOperationFuture>> future
			= getCreateDistributionGroupFuture(distributionGroup, distributionGroupConfiguration);
		
		// Don't retry the call, let the user check the logs
		return new EmptyResultFuture(future, FutureRetryPolicy.RETRY_POLICY_NONE);
	}

	/**
	 * @param distributionGroup
	 * @param distributionGroupConfiguration
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getCreateDistributionGroupFuture(final String distributionGroup,
			final DistributionGroupConfiguration distributionGroupConfiguration) {

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new DeleteDistributionGroupRequest(nextSequenceNumber, distributionGroup);
		};

		return () -> {
			final NetworkOperationFutureImpl future = new NetworkOperationFutureImpl(connection, packageSupplier);

			// Let future fail fast
			future.setTotalRetries(NetworkOperationFutureImpl.FAST_FAIL_RETRIES);

			return Arrays.asList(future);
		};
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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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
	public TupleListFuture queryRectangle(final String table, final Hyperrectangle boundingBox, 
			final List<UserDefinedFilterDefinition> udfs) {
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				table, boundingBox, false, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future
			= getQueryBoundingBoxFuture(table, boundingBox, routingHeader, udfs);

		return new TupleListFuture(future, new DoNothingDuplicateResolver(), table);
	}

	/**
	 *
	 * @param table
	 * @param boundingBox
	 * @param routingHeader
	 * @param customValue 
	 * @param filterName 
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getQueryBoundingBoxFuture(final String table,
			final Hyperrectangle boundingBox, final RoutingHeader routingHeader, 
			final List<UserDefinedFilterDefinition> udfs) {

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryHyperrectangleRequest(nextSequenceNumber,
					routingHeader, table, boundingBox, udfs, 
					pagingEnabled, tuplesPerPage);
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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final RoutingHeader routingHeaderSupplier = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
					queryPlan.getStreamTable(), queryPlan.getQueryRange(), false, connection.getServerAddress());

			final short nextSequenceNumber = connection.getNextSequenceNumber();
	
			return new QueryContinuousRequest(
					nextSequenceNumber, routingHeaderSupplier, queryPlan);
		};


		return () -> {
			final NetworkOperationFutureImpl future = new NetworkOperationFutureImpl(connection, packageSupplier);
			
			// Let the operation fail fast
			future.setTotalRetries(NetworkOperationFutureImpl.FAST_FAIL_RETRIES);
			
			return Arrays.asList(future);
		};
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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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
	public JoinedTupleListFuture querySpatialJoin(final List<String> tableNames, final Hyperrectangle boundingBox,
			final List<UserDefinedFilterDefinition> udfs) {
		
		final RoutingHeader routingHeader = RoutingHeaderHelper.getRoutingHeaderForLocalSystemReadNE(
				tableNames.get(0), boundingBox, true, connection.getServerAddress());

		final Supplier<List<NetworkOperationFuture>> future
			= getJoinFuture(tableNames, boundingBox, routingHeader, udfs);

		return new JoinedTupleListFuture(future);
	}

	/**
	 * @param tableNames
	 * @param boundingBox
	 * @param customValue 
	 * @param filterName 
	 * @param routingHeader2
	 * @return
	 */
	public Supplier<List<NetworkOperationFuture>> getJoinFuture(final List<String> tableNames, 
			final Hyperrectangle boundingBox, final RoutingHeader routingHeader, 
			final List<UserDefinedFilterDefinition> udfs) {

		final Supplier<NetworkRequestPacket> packageSupplier = () -> {

			final List<TupleStoreName> tupleStoreNames = tableNames
					.stream()
					.map(t -> new TupleStoreName(t))
					.collect(Collectors.toList());

			final short nextSequenceNumber = connection.getNextSequenceNumber();

			return new QueryJoinRequest(nextSequenceNumber, routingHeader, tupleStoreNames, 
					boundingBox, udfs, pagingEnabled, tuplesPerPage);
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
		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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
		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
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
		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new CancelRequest(nextSequenceNumber, queryPackageId);
		};

		return () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));
	}

	/**
	 * Cancel the given query
	 */
	public void cancelQuery(final Map<BBoxDBClient, List<Short>> cancelData)
			throws BBoxDBException, InterruptedException {

		BBoxDBClientHelper.cancelQuery(cancelData);
	}

	/**
	 * Get the continuous query state
	 */
	public ContinuousQueryServerStateFuture getContinuousQueryState(final TupleStoreName tupleStore) {
		
		final Supplier<NetworkRequestPacket> packageSupplier = () -> {
			final short nextSequenceNumber = connection.getNextSequenceNumber();
			return new ContinuousQueryStateRequest(nextSequenceNumber, tupleStore);
		};

		final Supplier<List<NetworkOperationFuture>> supplier = () -> Arrays.asList(new NetworkOperationFutureImpl(connection, packageSupplier));

		return new ContinuousQueryServerStateFuture(supplier);
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
		return true;
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
	 * Get the connection
	 * @return
	 */
	public BBoxDBConnection getConnection() {
		return connection;
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

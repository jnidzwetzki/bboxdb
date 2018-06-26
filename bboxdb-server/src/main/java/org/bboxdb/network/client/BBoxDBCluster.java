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
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.MicroSecondTimestampProvider;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.placement.RandomResourcePlacementStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.placement.ResourcePlacementStrategy;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.FutureRetryPolicy;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.NetworkOperationFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.network.routing.RoutingHopHelper;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBCluster implements BBoxDB {

	/**
	 * The Zookeeper connection
	 */
	private final ZookeeperClient zookeeperClient;

	/**
	 * The resource placement strategy
	 */
	private final ResourcePlacementStrategy resourcePlacementStrategy;

	/**
	 * The membership connection service
	 */
	private final MembershipConnectionService membershipConnectionService;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBCluster.class);

	/**
	 * Create a new instance of the BBoxDB cluster
	 * @param zookeeperNodes
	 * @param clustername
	 */
	public BBoxDBCluster(final Collection<String> zookeeperNodes, final String clustername) {
		zookeeperClient = new ZookeeperClient(zookeeperNodes, clustername);
		resourcePlacementStrategy = new RandomResourcePlacementStrategy();
		membershipConnectionService = MembershipConnectionService.getInstance();
	}

	/**
	 * Create a new instance of the BBoxDB cluster
	 * @param zookeeperNodes
	 * @param clustername
	 */
	public BBoxDBCluster(final String zookeeperNode, final String clustername) {
		this(Arrays.asList(zookeeperNode), clustername);
	}


	@Override
	public boolean connect() {
		zookeeperClient.init();
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		membershipConnectionService.init();
		return zookeeperClient.isConnected();
	}

	@Override
	public void close() {
		membershipConnectionService.shutdown();
		BBoxDBInstanceManager.getInstance().stopMembershipObserver();
		zookeeperClient.shutdown();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#createTable(java.lang.String)
	 */
	@Override
	public EmptyResultFuture createTable(final String table, final TupleStoreConfiguration configuration) 
			throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("createTable called, but connection list is empty");
		}

		try {
			final BBoxDBClient bboxdbClient = getSystemForNewRessources().getBboxDBClient();

			return bboxdbClient.createTable(table, configuration);
		} catch (ResourceAllocationException e) {
			throw new BBoxDBException(e);
		}
	}

	@Override
	public EmptyResultFuture deleteTable(final String deleteTable) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("deleteTable called, but connection list is empty");
		}

		try {
			final BBoxDBClient bboxdbClient = getSystemForNewRessources().getBboxDBClient();

			return bboxdbClient.deleteTable(deleteTable);
		} catch (ResourceAllocationException e) {
			throw new BBoxDBException(e);
		}
	}

	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {
		
		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> supplier = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForWrite(distributionRegion, 
					tuple.getBoundingBox());
			
			if(hops.isEmpty()) {
				logger.error("Got no hops for bbox {}", tuple.getBoundingBox());
				return new ArrayList<>();
			} else {
				final BBoxDBInstance instance = hops.get(0).getDistributedInstance();
				
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, hops);	
	
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getInsertTupleFuture(table, tuple, routingHeader);
				
				return Arrays.asList(future);
			}
		};
		
		return new EmptyResultFuture(supplier);
	}

	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key) throws BBoxDBException {
		final long timestamp = MicroSecondTimestampProvider.getNewTimestamp();
		return deleteTuple(table, key, timestamp);
	}
	
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) 
			throws BBoxDBException {
	
		return deleteTuple(table, key, timestamp, Hyperrectangle.FULL_SPACE);
	}

	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp, 
			final Hyperrectangle boundingBox) throws BBoxDBException {
		
		final DeletedTuple tuple = new DeletedTuple(key, timestamp);
		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> supplier = () -> {
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForWrite(distributionRegion, 
					boundingBox);
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getInsertTupleFuture(table, tuple, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		return new EmptyResultFuture(supplier);
	}
	
	@Override
	public EmptyResultFuture lockTuple(final String table, final Tuple tuple, 
			final boolean deleteOnTimeout) throws BBoxDBException {
		
		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> supplier = () -> {
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForWrite(distributionRegion, 
					tuple.getBoundingBox());
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().createLockTupleFuture(
							table, tuple, deleteOnTimeout, routingHeader);
				
				futures.add(future);
			}
			
			return futures;
		};
	
		// When version locking fails, try again with another version
		return new EmptyResultFuture(supplier, FutureRetryPolicy.RETRY_POLICY_NONE);
	}

	@Override
	public EmptyResultFuture createDistributionGroup(final String distributionGroup, 
			final DistributionGroupConfiguration distributionGroupConfiguration) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("createDistributionGroup called, but connection list is empty");
		}

		try {
			final BBoxDBClient bboxdbClient = getSystemForNewRessources().getBboxDBClient();

			return bboxdbClient.createDistributionGroup(distributionGroup, distributionGroupConfiguration);
		} catch (ResourceAllocationException e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Find a system with free resources
	 * @return
	 * @throws ResourceAllocationException 
	 */
	private BBoxDBConnection getSystemForNewRessources() throws ResourceAllocationException {
		final List<BBoxDBInstance> serverConnections = membershipConnectionService.getAllInstances();

		if(serverConnections == null) {
			throw new ResourceAllocationException("Server connections are null");
		}

		final BBoxDBInstance system = resourcePlacementStrategy.getInstancesForNewRessource(serverConnections);

		return membershipConnectionService.getConnectionForInstance(system);
	}

	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("deleteDistributionGroup called, but connection list is empty");
		}

		try {
			final BBoxDBClient bboxdbClient = getSystemForNewRessources().getBboxDBClient();

			return bboxdbClient.deleteDistributionGroup(distributionGroup);
		} catch (ResourceAllocationException e) {
			throw new BBoxDBException(e);
		}
	}

	@Override
	public TupleListFuture queryKey(final String table, final String key) throws BBoxDBException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for key {} in table {}", key, table);
		}
		
		final DeletedTuple tuple = new DeletedTuple(key);
		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> futureProvider = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForRead(distributionRegion, 
					tuple.getBoundingBox());
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getQueryKeyFuture(table, key, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		final DuplicateResolver<Tuple> duplicateResolver 
			= TupleStoreConfigurationCache.getInstance().getDuplicateResolverForTupleStore(table);

		return new TupleListFuture(futureProvider, duplicateResolver, table);
	}

	@Override
	public TupleListFuture queryRectangle(final String table, final Hyperrectangle boundingBox) throws BBoxDBException {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for bounding box {} in table {}", boundingBox, table);
		}
		
		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> futureProvider = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForRead(distributionRegion, 
					boundingBox);
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getQueryBoundingBoxFuture(table, boundingBox, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		return new TupleListFuture(futureProvider, new DoNothingDuplicateResolver(), table);
	}

	/**
	 * Execute a continuous bounding box query
	 * @throws BBoxDBException 
	 * 
	 */
	@Override
	public TupleListFuture queryRectangleContinuous(final String table, final Hyperrectangle boundingBox) 
			throws BBoxDBException {
	
		final DistributionRegion distributionRegion = getRootNode(table);

		final List<DistributionRegion> regions 
			= distributionRegion.getDistributionRegionsForBoundingBox(boundingBox);

		if(regions.size() != 1) {
			throw new BBoxDBException("The bounding box belongs to more than one distribution region, "
					+ "this is not supported in continuous queries");
		}

		final DistributionRegion region = regions.get(0);

		final BBoxDBInstance firstSystem = region.getSystems().get(0);
		final BBoxDBConnection connection = membershipConnectionService.getConnectionForInstance(firstSystem);

		return connection.getBboxDBClient().queryRectangleContinuous(table, boundingBox);
	}

	@Override
	public TupleListFuture queryRectangleAndTime(final String table,
			final Hyperrectangle boundingBox, final long timestamp) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryBoundingBoxAndTime called, but connection list is empty");
		}

		if(logger.isDebugEnabled()) {
			logger.debug("Query by for bounding box {} in table {}", boundingBox, table);
		}
		
		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> futureProvider = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForRead(distributionRegion, 
					boundingBox);
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getBoundingBoxAndTimeFuture(table, boundingBox, 
							timestamp, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		return new TupleListFuture(futureProvider, new DoNothingDuplicateResolver(), table);
	}

	@Override
	public TupleListFuture queryVersionTime(final String table, final long timestamp) throws BBoxDBException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryTime called, but connection list is empty");
		}

		if(logger.isDebugEnabled()) {
			logger.debug("Query by for timestamp {} in table {}", timestamp, table);
		}

		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> futureProvider = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForRead(distributionRegion, 
					Hyperrectangle.FULL_SPACE);
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getVersionTimeFuture(table, timestamp, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		return new TupleListFuture(futureProvider, new DoNothingDuplicateResolver(), table);
	}

	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) throws BBoxDBException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryTime called, but connection list is empty");
		}

		if(logger.isDebugEnabled()) {
			logger.debug("Query by for timestamp {} in table {}", timestamp, table);
		}

		final DistributionRegion distributionRegion = getRootNode(table);

		final Supplier<List<NetworkOperationFuture>> futureProvider = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForRead(distributionRegion, 
					Hyperrectangle.FULL_SPACE);
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getInsertedTimeFuture(table, 
							timestamp, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		return new TupleListFuture(futureProvider, new DoNothingDuplicateResolver(), table);
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.BBoxDB#queryJoin
	 */
	@Override
	public JoinedTupleListFuture queryJoin(final List<String> tableNames, final Hyperrectangle boundingBox) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryJoin called, but connection list is empty");
		}

		if(logger.isDebugEnabled()) {
			logger.debug("Query by for join {} on tables {}", boundingBox, tableNames);
		}
		
		
		final String fullname = tableNames.get(0);
		final DistributionRegion distributionRegion = getRootNode(fullname);
		
		final Supplier<List<NetworkOperationFuture>> futureProvider = () -> {
			
			final List<RoutingHop> hops = RoutingHopHelper.getRoutingHopsForRead(distributionRegion, 
					boundingBox);
		
			final List<NetworkOperationFuture> futures = new ArrayList<>();
			
			for(final RoutingHop hop : hops) {
				final BBoxDBInstance instance = hop.getDistributedInstance();
				final BBoxDBConnection connection 
					= membershipConnectionService.getConnectionForInstance(instance);
				final RoutingHeader routingHeader = new RoutingHeader((short) 0, Arrays.asList(hop));
				final NetworkOperationFuture future 
					= connection.getBboxDBClient().getJoinFuture(tableNames, 
							boundingBox, routingHeader);
				futures.add(future);
			}
			
			return futures;
		};

		return new JoinedTupleListFuture(futureProvider);		
	}

	/**
	 * Get the root node from space partitioner
	 * 
	 * @param fullname
	 * @return
	 * @throws BBoxDBException
	 */
	private DistributionRegion getRootNode(final String fullname) throws BBoxDBException {
		final TupleStoreName tupleStoreName = new TupleStoreName(fullname);
		final String distributionGroup = tupleStoreName.getDistributionGroup();
		
		final SpacePartitioner distributionAdapter = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(distributionGroup);

		return distributionAdapter.getRootNode();
	}

	@Override
	public boolean isConnected() {
		return (membershipConnectionService.getNumberOfConnections() > 0);
	}

	/**
	 * Is the paging for queries enables
	 * @return
	 */
	public boolean isPagingEnabled() {
		return membershipConnectionService.isPagingEnabled();
	}

	/**
	 * Enable or disable paging
	 * @param pagingEnabled
	 */
	public void setPagingEnabled(final boolean pagingEnabled) {
		membershipConnectionService.setPagingEnabled(pagingEnabled);
	}

	/**
	 * Get the amount of tuples per page
	 * @return
	 */
	public short getTuplesPerPage() {
		return membershipConnectionService.getTuplesPerPage();
	}

	/**
	 * Set the tuples per page
	 * @param tuplesPerPage
	 */
	public void setTuplesPerPage(final short tuplesPerPage) {
		membershipConnectionService.setTuplesPerPage(tuplesPerPage);
	}
	
	@Override
	public int getInFlightCalls() {
		return membershipConnectionService
				.getAllConnections()
				.stream()
				.mapToInt(BBoxDBConnection::getInFlightCalls)
				.sum();
	}

	/**
	 * Get the zookeeper client
	 * @return
	 */
	public ZookeeperClient getZookeeperClient() {
		return zookeeperClient;
	}
}

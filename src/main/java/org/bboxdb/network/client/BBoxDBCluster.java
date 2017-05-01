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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.placement.RandomResourcePlacementStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.placement.ResourcePlacementStrategy;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.SSTableNameListFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.util.MicroSecondTimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBCluster implements BBoxDB {
	
	/**
	 * The zookeeper connection
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The number of in flight requests
	 * @return
	 */
	protected volatile short maxInFlightCalls = MAX_IN_FLIGHT_CALLS;
	
	/**
	 * The resource placement strategy
	 */
	protected final ResourcePlacementStrategy resourcePlacementStrategy;
	
	/**
	 * The membership connection service
	 */
	protected final MembershipConnectionService membershipConnectionService;
	
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
		zookeeperClient.startMembershipObserver();
		membershipConnectionService.init();
		return zookeeperClient.isConnected();
	}

	@Override
	public void disconnect() {
		membershipConnectionService.shutdown();
		zookeeperClient.shutdown();		
	}

	@Override
	public EmptyResultFuture deleteTable(final String table) throws BBoxDBException {
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("deleteTable called, but connection list is empty");
		}
		
		final List<BBoxDBClient> connections = membershipConnectionService.getAllConnections();
		final EmptyResultFuture future = new EmptyResultFuture();

		for(final BBoxDBClient client : connections) {
			
			if(logger.isDebugEnabled()) {
				logger.debug("Send delete call for table {} to {}", table, client);
			}
			
			final EmptyResultFuture result = client.deleteTable(table);
			future.merge(result);
		}
		
		return future;
	}

	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws BBoxDBException {

		try {
			final SSTableName ssTableName = new SSTableName(table);
			
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForTableName(
					ssTableName, zookeeperClient);

			final DistributionRegion distributionRegion = distributionAdapter.getRootNode();
			
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(tuple.getBoundingBox());

			if(systems.isEmpty()) {
				throw new BBoxDBException("Insert tuple called, but system list for bounding box is empty: " 
						+ tuple.getBoundingBox() + ". State is: " + distributionRegion.getState());
			}
			
			// Determine the first system, it will route the request to the remaining systems
			final DistributedInstance system = systems.iterator().next();
			final BBoxDBClient connection = membershipConnectionService.getConnectionForInstance(system);
			
			if(connection == null) {
				logger.warn("Unable to insert tuple, no connection to system: ", system);
				return getFailedEmptyResultFuture();
			}
			
			return connection.insertTuple(table, tuple);
		} catch (ZookeeperException e) {
			logger.warn("Got exception while inserting tuple", e);
			return getFailedEmptyResultFuture();
		}
	}

	/**
	 * Create and return an empty result future
	 * @return
	 */
	protected EmptyResultFuture getFailedEmptyResultFuture() {
		final EmptyResultFuture future = new EmptyResultFuture(1);
		future.setFailedState();
		future.fireCompleteEvent();
		return future;
	}

	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key) throws BBoxDBException {
		final List<BBoxDBClient> connections = membershipConnectionService.getAllConnections();
		final long timestamp = MicroSecondTimestampProvider.getNewTimestamp();
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("deleteTuple called, but connection list is empty");
		}
		
		final EmptyResultFuture future = new EmptyResultFuture();
		
		for(final BBoxDBClient client : connections) {
			if(logger.isDebugEnabled()) {
				logger.debug("Send delete call for tuple {} on {} to {}", key, table, client);
			}
			
			final EmptyResultFuture result = client.deleteTuple(table, key, timestamp);
			future.merge(result);
		}
		
		return future;
	}
	
	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) throws BBoxDBException {
		final List<BBoxDBClient> connections = membershipConnectionService.getAllConnections();
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("deleteTuple called, but connection list is empty");
		}
		
		final EmptyResultFuture future = new EmptyResultFuture();
		
		for(final BBoxDBClient client : connections) {
			if(logger.isDebugEnabled()) {
				logger.debug("Send delete call for tuple {} on {} to {}", key, table, client);
			}
			
			final EmptyResultFuture result = client.deleteTuple(table, key, timestamp);
			future.merge(result);
		}
		
		return future;
	}

	@Override
	public SSTableNameListFuture listTables() {
		try {
			final BBoxDBClient bboxDBClient = getSystemForNewRessources();
			return bboxDBClient.listTables();
		} catch (ResourceAllocationException e) {
			logger.warn("listTables called, but no ressoures are available", e);
			final SSTableNameListFuture future = new SSTableNameListFuture(1);
			future.setFailedState();
			future.fireCompleteEvent();
			return future;
		}
	}

	@Override
	public EmptyResultFuture createDistributionGroup(final String distributionGroup, final short replicationFactor) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("createDistributionGroup called, but connection list is empty");
		}

		try {
			final BBoxDBClient bboxdbClient = getSystemForNewRessources();
			return bboxdbClient.createDistributionGroup(distributionGroup, replicationFactor);
		} catch (ResourceAllocationException e) {
			logger.warn("createDistributionGroup called, but no ressoures are available", e);
			return getFailedEmptyResultFuture();
		}
	}

	/**
	 * Find a system with free resources
	 * @return
	 * @throws ResourceAllocationException 
	 */
	protected BBoxDBClient getSystemForNewRessources() throws ResourceAllocationException {
		final List<DistributedInstance> serverConnections = membershipConnectionService.getAllInstances();
		
		if(serverConnections == null) {
			throw new ResourceAllocationException("Server connections are null");
		}
		
		final DistributedInstance system = resourcePlacementStrategy.getInstancesForNewRessource(serverConnections);
		return membershipConnectionService.getConnectionForInstance(system);
	}

	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) throws BBoxDBException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("deleteDistributionGroup called, but connection list is empty");
		}
		
		final EmptyResultFuture future = new EmptyResultFuture();
		
		for(final BBoxDBClient client : membershipConnectionService.getAllConnections()) {
			final EmptyResultFuture deleteFuture = client.deleteDistributionGroup(distributionGroup);
			future.merge(deleteFuture);
		}

		return future;
	}

	@Override
	public TupleListFuture queryKey(final String table, final String key) throws BBoxDBException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryKey called, but connection list is empty");
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for key {} in table {}", key, table);
		}
		
		for(final BBoxDBClient client : membershipConnectionService.getAllConnections()) {
			final TupleListFuture queryFuture = client.queryKey(table, key);
			
			if(queryFuture != null) {
				future.merge(queryFuture);
			}
			
		}

		return future;
	}

	@Override
	public TupleListFuture queryBoundingBox(final String table, final BoundingBox boundingBox) throws BBoxDBException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryBoundingBox called, but connection list is empty");
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		try {
			final SSTableName sstableName = new SSTableName(table);

			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForTableName(
					sstableName, zookeeperClient);

			final DistributionRegion distributionRegion = distributionAdapter.getRootNode();
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(boundingBox);
			
			if(logger.isDebugEnabled()) {
				logger.debug("Query by for bounding box {} in table {} on systems {}", boundingBox, table, systems);
			}
			
			for(final DistributedInstance system : systems) {
				final BBoxDBClient connection = membershipConnectionService.getConnectionForInstance(system);
				final TupleListFuture result = connection.queryBoundingBox(table, boundingBox);
				
				if(result != null) {
					future.merge(result);
				}
			}
			
		} catch (ZookeeperException e) {
			e.printStackTrace();
		}
		
		return future;
	}
	

	@Override
	public TupleListFuture queryBoundingBoxAndTime(final String table,
			final BoundingBox boundingBox, final long timestamp) throws BBoxDBException {
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryBoundingBoxAndTime called, but connection list is empty");
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		try {
			final SSTableName sstableName = new SSTableName(table);
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForTableName(
					sstableName, zookeeperClient);

			final DistributionRegion distributionRegion = distributionAdapter.getRootNode();
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(boundingBox);
			
			if(logger.isDebugEnabled()) {
				logger.debug("Query by for bounding box {} in table {} on systems {}", boundingBox, table, systems);
			}
			
			for(final DistributedInstance system : systems) {
				final BBoxDBClient connection = membershipConnectionService.getConnectionForInstance(system);
				final TupleListFuture result = connection.queryBoundingBoxAndTime(table, boundingBox, timestamp);
				
				if(result != null) {
					future.merge(result);
				}
			}
			
		} catch (ZookeeperException e) {
			e.printStackTrace();
		}
		
		return future;
	}

	@Override
	public TupleListFuture queryVersionTime(final String table, final long timestamp) throws BBoxDBException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryTime called, but connection list is empty");
		}
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for timestamp {} in table {}", timestamp, table);
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		for(final BBoxDBClient client : membershipConnectionService.getAllConnections()) {
			final TupleListFuture queryFuture = client.queryVersionTime(table, timestamp);
			
			if(queryFuture != null) {
				future.merge(queryFuture);
			}
		}

		return future;
	}
	
	@Override
	public TupleListFuture queryInsertedTime(final String table, final long timestamp) throws BBoxDBException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new BBoxDBException("queryTime called, but connection list is empty");
		}
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for timestamp {} in table {}", timestamp, table);
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		for(final BBoxDBClient client : membershipConnectionService.getAllConnections()) {
			final TupleListFuture queryFuture = client.queryInsertedTime(table, timestamp);
			
			if(queryFuture != null) {
				future.merge(queryFuture);
			}
		}

		return future;
	}

	@Override
	public boolean isConnected() {
		return (membershipConnectionService.getNumberOfConnections() > 0);
	}

	@Override
	public NetworkConnectionState getConnectionState() {
		return NetworkConnectionState.NETWORK_CONNECTION_OPEN;
	}

	@Override
	public int getInFlightCalls() {
		return membershipConnectionService
				.getAllConnections()
				.stream()
				.mapToInt(BBoxDBClient::getInFlightCalls)
				.sum();
	}

	@Override
	public short getMaxInFlightCalls() {
		return maxInFlightCalls;
	}

	@Override
	public void setMaxInFlightCalls(final short maxInFlightCalls) {
		this.maxInFlightCalls = maxInFlightCalls;
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
	

	//===============================================================
	// Test * Test * Test * Test * Test * Test * Test * Test
	//===============================================================
	public static void main(final String[] args) throws InterruptedException, ExecutionException, BBoxDBException {
		final String GROUP = "2_TESTGROUP";
		final String TABLE = "2_TESTGROUP_DATA";
		
		final Collection<String> zookeeperNodes = new ArrayList<String>();
		zookeeperNodes.add("node1:2181");
		final BBoxDBCluster bboxdbCluster = new BBoxDBCluster(zookeeperNodes, "mycluster");
		bboxdbCluster.connect();
		
		// Recreate distribution group
		final EmptyResultFuture futureDelete = bboxdbCluster.deleteDistributionGroup(GROUP);
		futureDelete.waitForAll();
		final EmptyResultFuture futureCreate = bboxdbCluster.createDistributionGroup(GROUP, (short) 2);
		futureCreate.waitForAll();
		
		// Insert the tuples
		final Random bbBoxRandom = new Random();
		for(int i = 0; i < 100000; i++) {
			final double x = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			final double y = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			
			final BoundingBox boundingBox = new BoundingBox(x, x+1, y, y+1);
			
			System.out.println("Inserting Tuple " + i + " : " + boundingBox);
			
			final EmptyResultFuture result = bboxdbCluster.insertTuple(TABLE, new Tuple(Integer.toString(i), boundingBox, "abcdef".getBytes()));
			result.waitForAll();
		}		
		
		bboxdbCluster.disconnect();
	}

}

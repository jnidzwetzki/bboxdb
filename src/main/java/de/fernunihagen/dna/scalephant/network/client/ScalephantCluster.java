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
package de.fernunihagen.dna.scalephant.network.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.scalephant.distribution.placement.RandomResourcePlacementStrategy;
import de.fernunihagen.dna.scalephant.distribution.placement.ResourceAllocationException;
import de.fernunihagen.dna.scalephant.distribution.placement.ResourcePlacementStrategy;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.scalephant.network.client.future.EmptyResultFuture;
import de.fernunihagen.dna.scalephant.network.client.future.SSTableNameListFuture;
import de.fernunihagen.dna.scalephant.network.client.future.TupleListFuture;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class ScalephantCluster implements Scalephant {
	
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
	private final static Logger logger = LoggerFactory.getLogger(ScalephantCluster.class);

	/**
	 * Create a new instance of the ScalepahntCluster 
	 * @param zookeeperNodes
	 * @param clustername
	 */
	public ScalephantCluster(final Collection<String> zookeeperNodes, final String clustername) {
		zookeeperClient = new ZookeeperClient(zookeeperNodes, clustername);
		resourcePlacementStrategy = new RandomResourcePlacementStrategy();
		membershipConnectionService = MembershipConnectionService.getInstance();
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
	public EmptyResultFuture deleteTable(final String table) throws ScalephantException {
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("deleteTable called, but connection list is empty");
		}
		
		final List<ScalephantClient> connections = membershipConnectionService.getAllConnections();
		final EmptyResultFuture future = new EmptyResultFuture();

		for(final ScalephantClient client : connections) {
			
			if(logger.isDebugEnabled()) {
				logger.debug("Send delete call for table {} to {}", table, client);
			}
			
			final EmptyResultFuture result = client.deleteTable(table);
			future.merge(result);
		}
		
		return future;
	}

	@Override
	public EmptyResultFuture insertTuple(final String table, final Tuple tuple) throws ScalephantException {

		try {
			final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForTableName(table, zookeeperClient);
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(tuple.getBoundingBox());

			if(systems.isEmpty()) {
				throw new ScalephantException("Insert tuple called, but system list for bounding box is empty: " + tuple.getBoundingBox());
			}
			
			// Determine the first system, it will route the request to the remaining systems
			final DistributedInstance system = systems.iterator().next();
			final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(system);
			return connection.insertTuple(table, tuple);
		} catch (ZookeeperException e) {
			logger.warn("Got exception while inserting tuple", e);
			final EmptyResultFuture future = new EmptyResultFuture(1);
			future.setFailedState();
			future.fireCompleteEvent();
			return future;
		}
	}

	@Override
	public EmptyResultFuture deleteTuple(final String table, final String key, final long timestamp) throws ScalephantException {
		final List<ScalephantClient> connections = membershipConnectionService.getAllConnections();
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("deleteTuple called, but connection list is empty");
		}
		
		final EmptyResultFuture future = new EmptyResultFuture();
		
		for(final ScalephantClient client : connections) {
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
			final ScalephantClient scalephantClient = getSystemForNewRessources();
			return scalephantClient.listTables();
		} catch (ResourceAllocationException e) {
			logger.warn("listTables called, but no ressoures are available", e);
			final SSTableNameListFuture future = new SSTableNameListFuture(1);
			future.setFailedState();
			future.fireCompleteEvent();
			return future;
		}
	}

	@Override
	public EmptyResultFuture createDistributionGroup(final String distributionGroup, final short replicationFactor) throws ScalephantException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("createDistributionGroup called, but connection list is empty");
		}

		try {
			final ScalephantClient scalephantClient = getSystemForNewRessources();
			return scalephantClient.createDistributionGroup(distributionGroup, replicationFactor);
		} catch (ResourceAllocationException e) {
			logger.warn("createDistributionGroup called, but no ressoures are available", e);
			final EmptyResultFuture future = new EmptyResultFuture(1);
			future.setFailedState();
			future.fireCompleteEvent();
			return future;
		}
	}

	/**
	 * Find a system with free resources
	 * @return
	 * @throws ResourceAllocationException 
	 */
	protected ScalephantClient getSystemForNewRessources() throws ResourceAllocationException {
		final List<DistributedInstance> serverConnections = membershipConnectionService.getAllInstances();
		final DistributedInstance system = resourcePlacementStrategy.getInstancesForNewRessource(serverConnections);
		return membershipConnectionService.getConnectionForInstance(system);
	}

	@Override
	public EmptyResultFuture deleteDistributionGroup(final String distributionGroup) throws ScalephantException {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("deleteDistributionGroup called, but connection list is empty");
		}
		
		final EmptyResultFuture future = new EmptyResultFuture();
		
		for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
			final EmptyResultFuture deleteFuture = client.deleteDistributionGroup(distributionGroup);
			future.merge(deleteFuture);
		}

		return future;
	}

	@Override
	public TupleListFuture queryKey(final String table, final String key) throws ScalephantException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("queryKey called, but connection list is empty");
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for key " + key + " in table " + table);
		}
		
		for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
			final TupleListFuture queryFuture = client.queryKey(table, key);
			future.merge(queryFuture);
		}

		return future;
	}

	@Override
	public TupleListFuture queryBoundingBox(final String table, final BoundingBox boundingBox) throws ScalephantException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("queryBoundingBox called, but connection list is empty");
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		try {
			final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForTableName(table, zookeeperClient);
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(boundingBox);
			
			if(logger.isDebugEnabled()) {
				logger.debug("Query by for bounding box {} in table {} on systems {}", boundingBox, table, systems);
			}
			
			for(final DistributedInstance system : systems) {
				final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(system);
				final TupleListFuture result = connection.queryBoundingBox(table, boundingBox);
				future.merge(result);
			}
			
		} catch (ZookeeperException e) {
			e.printStackTrace();
		}
		
		return future;
		
	}

	@Override
	public TupleListFuture queryTime(final String table, final long timestamp) throws ScalephantException {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			throw new ScalephantException("queryTime called, but connection list is empty");
		}
		
		if(logger.isDebugEnabled()) {
			logger.debug("Query by for timestamp {} in table {}", timestamp, table);
		}
		
		final TupleListFuture future = new TupleListFuture();
		
		for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
			final TupleListFuture deleteFuture = client.queryTime(table, timestamp);
			future.merge(deleteFuture);
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
				.mapToInt(ScalephantClient::getInFlightCalls)
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
	public static void main(final String[] args) throws InterruptedException, ExecutionException, ScalephantException {
		final String GROUP = "2_TESTGROUP";
		final String TABLE = "2_TESTGROUP_DATA";
		
		final Collection<String> zookeeperNodes = new ArrayList<String>();
		zookeeperNodes.add("node1:2181");
		final ScalephantCluster scalephantCluster = new ScalephantCluster(zookeeperNodes, "mycluster");
		scalephantCluster.connect();
		
		// Recreate distribution group
		final EmptyResultFuture futureDelete = scalephantCluster.deleteDistributionGroup(GROUP);
		futureDelete.waitForAll();
		final EmptyResultFuture futureCreate = scalephantCluster.createDistributionGroup(GROUP, (short) 2);
		futureCreate.waitForAll();
		
		// Insert the tuples
		final Random bbBoxRandom = new Random();
		for(int i = 0; i < 100000; i++) {
			final float x = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			final float y = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			
			final BoundingBox boundingBox = new BoundingBox(x, x+1, y, y+1);
			
			System.out.println("Inserting Tuple " + i + " : " + boundingBox);
			
			final EmptyResultFuture result = scalephantCluster.insertTuple(TABLE, new Tuple(Integer.toString(i), boundingBox, "abcdef".getBytes()));
			result.waitForAll();
		}		
		
		scalephantCluster.disconnect();
	}

}

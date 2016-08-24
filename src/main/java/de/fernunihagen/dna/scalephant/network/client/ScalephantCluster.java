package de.fernunihagen.dna.scalephant.network.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	 * The pending calls
	 */
	protected final Map<Short, ClientOperationFuture> pendingCalls = new HashMap<Short, ClientOperationFuture>();

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
	public boolean disconnect() {
		membershipConnectionService.shutdown();
		zookeeperClient.shutdown();		
		return true;
	}

	@Override
	public OperationFuture deleteTable(final String table) {
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("deleteTable called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
		
		final MultiClientOperationFuture future = new MultiClientOperationFuture();
		final List<ScalephantClient> connections = membershipConnectionService.getAllConnections();
		for(final ScalephantClient client : connections) {
			
			if(logger.isDebugEnabled()) {
				logger.debug("Send delete call for table " + table + " to " + client);
			}
			
			final ClientOperationFuture result = client.deleteTable(table);
			future.addFuture(result);
		}
		
		return future;
	}

	@Override
	public OperationFuture insertTuple(final String table, final Tuple tuple) {

		try {
			final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForTableName(table, zookeeperClient);
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(tuple.getBoundingBox());

			if(systems.isEmpty()) {
				logger.warn("Insert tuple called, but system list is empty");
				final ClientOperationFuture future = new ClientOperationFuture();
				future.setFailedState();
				return future;
			}
			
			// Determine the first system, it will route the request to the remaining systems
			final DistributedInstance system = systems.iterator().next();
			final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(system);
			return connection.insertTuple(table, tuple);
		} catch (ZookeeperException e) {
			logger.warn("Got exception while inserting tuple", e);
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
	}

	@Override
	public OperationFuture deleteTuple(final String table, final String key) {
		final List<ScalephantClient> connections = membershipConnectionService.getAllConnections();
		
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("deleteTuple called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
		
		final MultiClientOperationFuture future = new MultiClientOperationFuture();
		
		for(final ScalephantClient client : connections) {
			if(logger.isDebugEnabled()) {
				logger.debug("Send delete call for table " + table + " to " + client);
			}
			
			final ClientOperationFuture result = client.deleteTuple(table, key);
			future.addFuture(result);
		}
		
		return future;
	}

	@Override
	public OperationFuture listTables() {
		try {
			final ScalephantClient scalephantClient = getSystemForNewRessources();
			return scalephantClient.listTables();
		} catch (ResourceAllocationException e) {
			logger.warn("listTables called, but no ressoures are available", e);
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
	}

	@Override
	public OperationFuture createDistributionGroup(final String distributionGroup, final short replicationFactor) {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("createDistributionGroup called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}

		try {
			final ScalephantClient scalephantClient = getSystemForNewRessources();
			return scalephantClient.createDistributionGroup(distributionGroup, replicationFactor);
		} catch (ResourceAllocationException e) {
			logger.warn("createDistributionGroup called, but no ressoures are available", e);
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
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
	public OperationFuture deleteDistributionGroup(final String distributionGroup) {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("deleteDistributionGroup called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
		
		final MultiClientOperationFuture future = new MultiClientOperationFuture();
		
		for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
			final ClientOperationFuture deleteFuture = client.deleteDistributionGroup(distributionGroup);
			future.addFuture(deleteFuture);
		}

		return future;
	}

	@Override
	public OperationFuture queryKey(final String table, final String key) {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("queryKey called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
		
		final MultiClientOperationFuture future = new MultiClientOperationFuture();
		
		for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
			final ClientOperationFuture deleteFuture = client.queryKey(table, key);
			future.addFuture(deleteFuture);
		}

		return future;
	}

	@Override
	public OperationFuture queryBoundingBox(final String table, final BoundingBox boundingBox) {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("queryBoundingBox called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
		
		final MultiClientOperationFuture future = new MultiClientOperationFuture();
		
		try {
			final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForTableName(table, zookeeperClient);
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(boundingBox);
			logger.info("Query tuple on systems: " + systems);
			
			for(final DistributedInstance system : systems) {
				final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(system);
				final ClientOperationFuture result = connection.queryBoundingBox(table, boundingBox);
				future.addFuture(result);
			}
			
		} catch (ZookeeperException e) {
			e.printStackTrace();
		}
		
		return future;
		
	}

	@Override
	public OperationFuture queryTime(final String table, final long timestamp) {
		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("queryTime called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		}
		
		final MultiClientOperationFuture future = new MultiClientOperationFuture();
		
		for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
			final ClientOperationFuture deleteFuture = client.queryTime(table, timestamp);
			future.addFuture(deleteFuture);
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
		synchronized (pendingCalls) {
			return pendingCalls.size();
		}
	}

	@Override
	public short getMaxInFlightCalls() {
		return maxInFlightCalls;
	}

	@Override
	public void setMaxInFlightCalls(final short maxInFlightCalls) {
		this.maxInFlightCalls = maxInFlightCalls;
	}
	

	
	//===============================================================
	// Test * Test * Test * Test * Test * Test * Test * Test
	//===============================================================
	public static void main(final String[] args) throws InterruptedException, ExecutionException {
		final String GROUP = "2_TESTGROUP";
		final String TABLE = "2_TESTGROUP_DATA";
		
		final Collection<String> zookeeperNodes = new ArrayList<String>();
		zookeeperNodes.add("node1:2181");
		final ScalephantCluster scalephantCluster = new ScalephantCluster(zookeeperNodes, "mycluster");
		scalephantCluster.connect();
		
		// Recreate distribution group
		final OperationFuture futureDelete = scalephantCluster.deleteDistributionGroup(GROUP);
		futureDelete.waitForAll();
		final OperationFuture futureCreate = scalephantCluster.createDistributionGroup(GROUP, (short) 2);
		futureCreate.waitForAll();
		
		// Insert the tuples
		final Random bbBoxRandom = new Random();
		for(int i = 0; i < 100000; i++) {
			final float x = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			final float y = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			
			final BoundingBox boundingBox = new BoundingBox(x, x+1, y, y+1);
			
			System.out.println("Inserting Tuple " + i + " : " + boundingBox);
			
			final OperationFuture result = scalephantCluster.insertTuple(TABLE, new Tuple(Integer.toString(i), boundingBox, "abcdef".getBytes()));
			result.waitForAll();
		}		
		
		scalephantCluster.disconnect();
	}

}

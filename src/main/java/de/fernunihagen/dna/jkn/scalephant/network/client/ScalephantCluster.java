package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.jkn.scalephant.distribution.resource.RandomResourcePlacementStrategy;
import de.fernunihagen.dna.jkn.scalephant.distribution.resource.ResourcePlacementStrategy;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

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
		membershipConnectionService = new MembershipConnectionService();
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
	public ClientOperationFuture deleteTable(String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientOperationFuture insertTuple(String table, Tuple tuple) {
		
		// FIXME: Demo Implementation 
		ClientOperationFuture result = null;

		try {
			final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForTableName(table, zookeeperClient);
			final Collection<DistributedInstance> systems = distributionRegion.getSystemsForBoundingBox(tuple.getBoundingBox());
			logger.info("Writing tuple to systems: " + systems);
			
			
			for(final DistributedInstance system : systems) {
				logger.info("Sending call to:  " + system);
	
				final ScalephantClient connection = membershipConnectionService.getConnectionForInstance(system);
				result = connection.insertTuple(table, tuple);
			}
			
		} catch (ZookeeperException e) {
			e.printStackTrace();
		}
		
		return result;
	}

	@Override
	public ClientOperationFuture deleteTuple(String table, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientOperationFuture listTables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientOperationFuture createDistributionGroup(
			final String distributionGroup, final short replicationFactor) {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("createDistributionGroup called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		} else {
			final ScalephantClient scalephantClient = getSystemForNewRessources();
			return scalephantClient.createDistributionGroup(distributionGroup, replicationFactor);
		}
	}

	/**
	 * Find a system with free resources
	 * @return
	 */
	protected ScalephantClient getSystemForNewRessources() {
		final List<DistributedInstance> serverConnections = membershipConnectionService.getAllInstances();
		final DistributedInstance system = resourcePlacementStrategy.findSystemToAllocate(serverConnections);
		return membershipConnectionService.getConnectionForInstance(system);
	}
	

	@Override
	public ClientOperationFuture deleteDistributionGroup(
			final String distributionGroup) {

		if(membershipConnectionService.getNumberOfConnections() == 0) {
			logger.warn("deleteDistributionGroup called, but connection list is empty");
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setFailedState();
			return future;
		} else {
			for(final ScalephantClient client : membershipConnectionService.getAllConnections()) {
				client.deleteDistributionGroup(distributionGroup);
			}

			// FIXME: 
			final ClientOperationFuture future = new ClientOperationFuture();
			future.setOperationResult("");
			return future;
		}
	}

	@Override
	public ClientOperationFuture queryKey(String table, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientOperationFuture queryBoundingBox(String table,
			BoundingBox boundingBox) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientOperationFuture queryTime(String table, long timestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isConnected() {
		return (membershipConnectionService.getNumberOfConnections() > 0);
	}

	@Override
	public NetworkConnectionState getConnectionState() {
		// TODO Auto-generated method stub
		return null;
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
		final ClientOperationFuture futureDelete = scalephantCluster.deleteDistributionGroup(GROUP);
		futureDelete.get();
		final ClientOperationFuture futureCreate = scalephantCluster.createDistributionGroup(GROUP, (short) 2);
		futureCreate.get();
		
		
		// Insert the tuples
		final Random bbBoxRandom = new Random();
		for(int i = 0; i < 100000; i++) {
			final float x = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			final float y = (float) Math.abs(bbBoxRandom.nextFloat() % 100000.0 * 1000);
			
			final BoundingBox boundingBox = new BoundingBox(x, x+1, y, y+1);
			
			System.out.println("Inserting Tuple " + i + " : " + boundingBox);
			
			final ClientOperationFuture result = scalephantCluster.insertTuple(TABLE, new Tuple(Integer.toString(i), boundingBox, "abcdef".getBytes()));
			result.get();
		}		
		
		scalephantCluster.disconnect();
	}

}

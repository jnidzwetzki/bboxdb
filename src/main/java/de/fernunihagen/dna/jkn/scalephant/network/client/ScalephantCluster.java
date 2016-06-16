package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceAddEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceDeleteEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEventCallback;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class ScalephantCluster implements Scalephant, DistributedInstanceEventCallback {
	
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
	 * The server connections
	 */
	protected final Map<DistributedInstance, ScalephantClient> serverConnections;
	
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
		serverConnections = new HashMap<DistributedInstance, ScalephantClient>();
	}

	@Override
	public boolean connect() {
		zookeeperClient.init();
		DistributedInstanceManager.getInstance().registerListener(this);
		zookeeperClient.startMembershipObserver();
		return zookeeperClient.isConnected();
	}

	@Override
	public boolean disconnect() {
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
		return null;
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
			String distributionGroup, short replicationFactor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientOperationFuture deleteDistributionGroup(
			String distributionGroup) {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public NetworkConnectionState getConnectionState() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getInFlightCalls() {
		// TODO Auto-generated method stub
		return 0;
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
	 * Add a new connection to a scalephant system
	 * @param distributedInstance
	 */
	protected synchronized void createConnection(final DistributedInstance distributedInstance) {
		logger.info("Opening connection to new node: " + distributedInstance);
		
		if(serverConnections.containsKey(distributedInstance)) {
			logger.info("We allready have a connection to: " + distributedInstance);
			return;
		}
		
		final ScalephantClient client = new ScalephantClient(distributedInstance.getInetSocketAddress());
		final boolean result = client.connect();
		
		if(! result) {
			logger.info("Unable to open connection to: " + distributedInstance);
		} else {
			logger.info("Connection successfully established: " + distributedInstance);
			serverConnections.put(distributedInstance, client);
		}
	}
	
	/**
	 * Terminate the connection to a missing scalepahnt system
	 * @param distributedInstance 
	 */
	protected synchronized void terminateConnection(final DistributedInstance distributedInstance) {
		
		logger.info("Closing connections to terminating node: " + distributedInstance);
		
		if(! serverConnections.containsKey(distributedInstance)) {
			return;
		}
		
		final ScalephantClient client = serverConnections.remove(distributedInstance);
		client.disconnect();
	}

	/**
	 * Handle membership events	
	 */
	@Override
	public void distributedInstanceEvent(final DistributedInstanceEvent event) {
		if(event instanceof DistributedInstanceAddEvent) {
			createConnection(event.getInstance());
		} else if(event instanceof DistributedInstanceDeleteEvent) {
			terminateConnection(event.getInstance());
		} else {
			logger.warn("Unknown event: " + event);
		}
	}
	
	//===============================================================
	// Test * Test * Test * Test * Test * Test * Test * Test
	//===============================================================
	public static void main(final String[] args) throws InterruptedException {
		final Collection<String> zookeeperNodes = new ArrayList<String>();
		zookeeperNodes.add("node1:2181");
		final ScalephantCluster scalephantCluster = new ScalephantCluster(zookeeperNodes, "mycluster");
		scalephantCluster.connect();
		
		Thread.sleep(10000000);
	}

}

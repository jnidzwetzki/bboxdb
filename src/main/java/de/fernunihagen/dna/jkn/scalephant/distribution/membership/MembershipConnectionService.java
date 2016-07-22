package de.fernunihagen.dna.jkn.scalephant.distribution.membership;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantService;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceAddEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceDeleteEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEventCallback;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantClient;

public class MembershipConnectionService implements ScalephantService, DistributedInstanceEventCallback {
	
	/**
	 * The server connections
	 */
	protected final Map<DistributedInstance, ScalephantClient> serverConnections;
	
	/**
	 * The blacklisted instances, no connection will be created to these systems
	 */
	protected Set<DistributedInstance> blacklist = new HashSet<>();
	
	protected static MembershipConnectionService instance = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MembershipConnectionService.class);
	
	private MembershipConnectionService() {
		final HashMap<DistributedInstance, ScalephantClient> connectionMap = new HashMap<DistributedInstance, ScalephantClient>();
		serverConnections = Collections.synchronizedMap(connectionMap);
	}
	
	/**
	 * Get the instance of the membership connection service
	 * @return
	 */
	public static synchronized MembershipConnectionService getInstance() {
		if(instance == null) {
			instance = new MembershipConnectionService();
		}
		
		return instance;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("Unable to clone a singleton");
	}

	/**
	 * Add a system to the blacklist
	 * @param distributedInstance
	 */
	public void addSystemToBlacklist(final DistributedInstance distributedInstance) {
		blacklist.add(distributedInstance);
	}
	
	/**
	 * Init the subsystem
	 */
	@Override
	public void init() {
		DistributedInstanceManager.getInstance().registerListener(this);
		
		// Create connections to existing instances
		final Set<DistributedInstance> instances = DistributedInstanceManager.getInstance().getInstances();
		
		if(instances.isEmpty()) {
			logger.warn("The list of instances is empty");
		}
		
		for(DistributedInstance distributedInstance : instances) {
			createConnection(distributedInstance);
		}
	}

	/**
	 * Shutdown the subsystem
	 */
	@Override
	public void shutdown() {
		DistributedInstanceManager.getInstance().removeListener(this);
		
		// Close all connections
		synchronized (serverConnections) {
			for(final DistributedInstance instance : serverConnections.keySet()) {
				final ScalephantClient client = serverConnections.get(instance);
				logger.info("Disconnecting from: " + instance);
				client.disconnect();
			}
			
			serverConnections.clear();
		}
	}

	/**
	 * Get the name for the subsystem
	 */
	@Override
	public String getServicename() {
		return "Mambership Connection Service";
	}
	
	/**
	 * Add a new connection to a scalephant system
	 * @param distributedInstance
	 */
	protected synchronized void createConnection(final DistributedInstance distributedInstance) {

		if(serverConnections.containsKey(distributedInstance)) {
			logger.info("We already have a connection to: " + distributedInstance);
			return;
		}
		
		if(blacklist.contains(distributedInstance)) {
			logger.info("Not creating a connection to the blacklisted sysetm: " + distributedInstance);
			return;
		}
		
		logger.info("Opening connection to new node: " + distributedInstance);
		
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
	 * Terminate the connection to a missing scalephant system
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

	/**
	 * Get the connection for the instance
	 * @param instance
	 * @return
	 */
	public ScalephantClient getConnectionForInstance(final DistributedInstance instance) {
		return serverConnections.get(instance);
	}
	
	/**
	 * Return the number of connections
	 * @return
	 */
	public int getNumberOfConnections() {
		return serverConnections.size();
	}
	
	/**
	 * Get all connections
	 * @return 
	 */
	public List<ScalephantClient> getAllConnections() {
		return new ArrayList<ScalephantClient>(serverConnections.values());
	}
	
	/**
	 * Get a list with all distributed instances we have connections to
	 * @return
	 */
	public List<DistributedInstance> getAllInstances() {
		return new ArrayList<DistributedInstance>(serverConnections.keySet());
	}
}

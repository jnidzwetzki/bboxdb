package de.fernunihagen.dna.scalephant.distribution.membership;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantService;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceAddEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceChangedEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceDeleteEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceEventCallback;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceState;
import de.fernunihagen.dna.scalephant.network.client.ScalephantClient;

public class MembershipConnectionService implements ScalephantService, DistributedInstanceEventCallback {
	
	/**
	 * The server connections
	 */
	protected final Map<InetSocketAddress, ScalephantClient> serverConnections;
	
	/**
	 * The known instances
	 */
	protected final Map<InetSocketAddress, DistributedInstance> knownInstances;
	
	/**
	 * The blacklisted instances, no connection will be created to these systems
	 */
	protected Set<InetSocketAddress> blacklist = new HashSet<>();
	
	protected static MembershipConnectionService instance = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MembershipConnectionService.class);
	
	private MembershipConnectionService() {
		final HashMap<InetSocketAddress, ScalephantClient> connectionMap = new HashMap<InetSocketAddress, ScalephantClient>();
		serverConnections = Collections.synchronizedMap(connectionMap);
		
		final HashMap<InetSocketAddress, DistributedInstance> instanceMap = new HashMap<InetSocketAddress, DistributedInstance>();
		knownInstances = Collections.synchronizedMap(instanceMap);
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
		blacklist.add(distributedInstance.getInetSocketAddress());
	}
	
	/**
	 * Init the subsystem
	 */
	@Override
	public void init() {
		DistributedInstanceManager.getInstance().registerListener(this);
		
		// Create connections to existing instances
		final List<DistributedInstance> instances = DistributedInstanceManager.getInstance().getInstances();
		
		if(instances.isEmpty()) {
			logger.warn("The list of instances is empty");
		}
		
		for(final DistributedInstance distributedInstance : instances) {
			createOrTerminateConnetion(distributedInstance);
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
			for(final InetSocketAddress instance : serverConnections.keySet()) {
				final ScalephantClient client = serverConnections.get(instance);
				logger.info("Closing connection to server: " + instance);
				client.closeConnection();
			}
			
			serverConnections.clear();
			knownInstances.clear();
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
	protected synchronized void createOrTerminateConnetion(final DistributedInstance distributedInstance) {

		// Create only connections to readonly or readwrite systems
		if(distributedInstance.getState() == DistributedInstanceState.UNKNOWN) {			
			terminateConnection(distributedInstance);
		} else {
			createConnection(distributedInstance);
		}
	}


	/**
	 * Create a new connection to the given instance
	 * @param distributedInstance
	 */
	protected void createConnection(final DistributedInstance distributedInstance) {
		
		if(serverConnections.containsKey(distributedInstance.getInetSocketAddress())) {
			logger.info("We have already a connection to: " + distributedInstance);
			return;
		}
		
		if(blacklist.contains(distributedInstance.getInetSocketAddress())) {
			logger.info("Not creating a connection to the blacklisted sysetm: " + distributedInstance);
			return;
		}
		
		logger.info("Opening connection to instance: " + distributedInstance);
		
		final ScalephantClient client = new ScalephantClient(distributedInstance.getInetSocketAddress());
		final boolean result = client.connect();
		
		if(! result) {
			logger.info("Unable to open connection to: " + distributedInstance);
		} else {
			logger.info("Connection successfully established: " + distributedInstance);
			serverConnections.put(distributedInstance.getInetSocketAddress(), client);
			knownInstances.put(distributedInstance.getInetSocketAddress(), distributedInstance);
		}
	}
	
	/**
	 * Terminate the connection to a missing scalephant system
	 * @param distributedInstance 
	 */
	protected synchronized void terminateConnection(final DistributedInstance distributedInstance) {
		
		if(! serverConnections.containsKey(distributedInstance.getInetSocketAddress())) {
			return;
		}
		
		logger.info("Closing connection to dead instance: " + distributedInstance);
		
		knownInstances.remove(distributedInstance.getInetSocketAddress());
		final ScalephantClient client = serverConnections.remove(distributedInstance.getInetSocketAddress());
		client.disconnect();
	}

	/**
	 * Handle membership events	
	 */
	@Override
	public void distributedInstanceEvent(final DistributedInstanceEvent event) {
		if(event instanceof DistributedInstanceAddEvent) {
			createOrTerminateConnetion(event.getInstance());
		} else if(event instanceof DistributedInstanceChangedEvent) {
			createOrTerminateConnetion(event.getInstance());
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
		return serverConnections.get(instance.getInetSocketAddress());
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
		return new ArrayList<DistributedInstance>(knownInstances.values());
	}
}

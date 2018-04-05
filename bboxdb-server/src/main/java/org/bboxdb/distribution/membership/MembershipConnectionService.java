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
package org.bboxdb.distribution.membership;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MembershipConnectionService implements BBoxDBService {
	
	/**
	 * The server connections
	 */
	private final Map<InetSocketAddress, BBoxDBConnection> serverConnections;
	
	/**
	 * The known instances
	 */
	private final Map<InetSocketAddress, BBoxDBInstance> knownInstances;
	
	/**
	 * The blacklisted instances, no connection will be created to these systems
	 */
	private final Set<InetSocketAddress> blacklist;
	
	/**
	 * Is the paging for queries enabled?
	 */
	private boolean pagingEnabled;
	
	/**
	 * The amount of tuples per page
	 */
	private short tuplesPerPage;
	
	/**
	 * The tuple store manager registry (used for gossip, between server<->server connections)
	 */
	private TupleStoreManagerRegistry tupleStoreManagerRegistry;
	
	/**
	 * The event handler
	 */
	private BiConsumer<DistributedInstanceEvent, BBoxDBInstance> distributedEventConsumer 
		= this::handleDistributedEvent;
	
	/**
	 * The singleton instance
	 */
	private static MembershipConnectionService instance = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MembershipConnectionService.class);
	
	private MembershipConnectionService() {
		this.serverConnections = new ConcurrentHashMap<>();		
		this.knownInstances = new ConcurrentHashMap<>();
		this.pagingEnabled = false;
		this.tuplesPerPage = 0;
		this.blacklist = new HashSet<>();
	}
	
	/**
	* Handle membership events	
	 * @param instance 
	*/
	private void handleDistributedEvent(final DistributedInstanceEvent event, final BBoxDBInstance instance) {
		if(event == DistributedInstanceEvent.ADD) {
			createOrTerminateConnetion(instance);
		} else if(event == DistributedInstanceEvent.CHANGED) {
			createOrTerminateConnetion(instance);
		} else if(event == DistributedInstanceEvent.DELETED) {
			terminateConnection(instance);
		} else {
			logger.warn("Unknown event: " + event);
		}		
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
	public void addSystemToBlacklist(final BBoxDBInstance distributedInstance) {
		blacklist.add(distributedInstance.getInetSocketAddress());
	}
	
	/**
	 * Clear the blacklist
	 */
	public void clearBlacklist() {
		blacklist.clear();
	}
	
	/**
	 * Init the subsystem
	 */
	@Override
	public void init() {
		BBoxDBInstanceManager.getInstance().registerListener(distributedEventConsumer);
		
		// Create connections to existing instances
		final List<BBoxDBInstance> instances = BBoxDBInstanceManager.getInstance().getInstances();
		
		if(instances.isEmpty()) {
			logger.warn("The list of instances is empty");
		}
		
		instances.forEach(i -> createOrTerminateConnetion(i));
	}

	/**
	 * Shutdown the subsystem
	 */
	@Override
	public void shutdown() {
		BBoxDBInstanceManager.getInstance().removeListener(distributedEventConsumer);
		
		// Close all connections
		synchronized (serverConnections) {
			for(final InetSocketAddress instance : serverConnections.keySet()) {
				final BBoxDBConnection client = serverConnections.get(instance);
				logger.info("Closing connection to server: " + instance);
				client.disconnect();
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
		return "Membership Connection Service";
	}
	
	/**
	 * Add a new connection to a bboxdb system
	 * @param distributedInstance
	 */
	private synchronized void createOrTerminateConnetion(final BBoxDBInstance distributedInstance) {

		// Create only connections to readonly or readwrite systems
		if(distributedInstance.getState() == BBoxDBInstanceState.FAILED) {			
			terminateConnection(distributedInstance);
		} else {
			createConnection(distributedInstance);
		}
	}

	/**
	 * Create a new connection to the given instance
	 * @param distributedInstance
	 */
	private void createConnection(final BBoxDBInstance distributedInstance) {
		
		final String instanceName = distributedInstance.getStringValue();
		
		if(serverConnections.containsKey(distributedInstance.getInetSocketAddress())) {
			logger.info("We have already a connection to: {}", instanceName);
			return;
		}
		
		if(blacklist.contains(distributedInstance.getInetSocketAddress())) {
			logger.info("Not creating a connection to the blacklisted system: {}", instanceName);
			return;
		}
		
		logger.info("Opening connection to instance: {}", instanceName);
		
		final BBoxDBConnection connection = new BBoxDBConnection(distributedInstance.getInetSocketAddress());
		final BBoxDBClient client = connection.getBboxDBClient();
		
		client.setPagingEnabled(pagingEnabled);
		client.setTuplesPerPage(tuplesPerPage);
		client.setTupleStoreManagerRegistry(tupleStoreManagerRegistry);
		
		final boolean result = connection.connect();
		
		if(! result) {
			logger.info("Unable to open connection to: {}", instanceName);
		} else {
			logger.info("Connection successfully established: {}", instanceName);
			serverConnections.put(distributedInstance.getInetSocketAddress(), connection);
			knownInstances.put(distributedInstance.getInetSocketAddress(), distributedInstance);
		}
	}
	
	/**
	 * Terminate the connection to a missing bboxdb system
	 * @param distributedInstance 
	 */
	private synchronized void terminateConnection(final BBoxDBInstance distributedInstance) {
		
		final String instanceName = distributedInstance.getStringValue();
		
		if(! serverConnections.containsKey(distributedInstance.getInetSocketAddress())) {
			return;
		}
		
		logger.info("Closing connection to dead instance: {}", instanceName);
		
		knownInstances.remove(distributedInstance.getInetSocketAddress());
		final BBoxDBConnection connection = serverConnections.remove(distributedInstance.getInetSocketAddress());
		connection.terminateConnection();
	}

	/**
	 * Get the connection for the instance
	 * @param instance
	 * @return
	 */
	public BBoxDBConnection getConnectionForInstance(final BBoxDBInstance instance) {
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
	public List<BBoxDBConnection> getAllConnections() {
		return new ArrayList<>(serverConnections.values());
	}
	
	/**
	 * Get a list with all distributed instances we have connections to
	 * @return
	 */
	public List<BBoxDBInstance> getAllInstances() {
		return new ArrayList<BBoxDBInstance>(knownInstances.values());
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
		serverConnections.values().forEach(c -> c.getBboxDBClient().setPagingEnabled(pagingEnabled));
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
		serverConnections.values().forEach(c -> c.getBboxDBClient().setTuplesPerPage(tuplesPerPage));
	}

	/**
	 * Get the tuple store manager registry (used for gossip in keep alive)
	 * @return
	 */
	public TupleStoreManagerRegistry getTupleStoreManagerRegistry() {
		return tupleStoreManagerRegistry;
	}

	/**
	 * Get the tuple store manager registry (used for gossip in keep alive)
	 * @param tupleStoreManagerRegistry
	 */
	public void setTupleStoreManagerRegistry(TupleStoreManagerRegistry tupleStoreManagerRegistry) {
		this.tupleStoreManagerRegistry = tupleStoreManagerRegistry;
	}	
}

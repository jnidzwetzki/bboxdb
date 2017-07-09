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
package org.bboxdb.distribution.membership;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bboxdb.distribution.membership.event.DistributedInstanceAddEvent;
import org.bboxdb.distribution.membership.event.DistributedInstanceChangedEvent;
import org.bboxdb.distribution.membership.event.DistributedInstanceDeleteEvent;
import org.bboxdb.distribution.membership.event.DistributedInstanceEvent;
import org.bboxdb.distribution.membership.event.DistributedInstanceEventCallback;
import org.bboxdb.distribution.membership.event.DistributedInstanceState;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MembershipConnectionService implements BBoxDBService, DistributedInstanceEventCallback {
	
	/**
	 * The server connections
	 */
	protected final Map<InetSocketAddress, BBoxDBClient> serverConnections;
	
	/**
	 * The known instances
	 */
	protected final Map<InetSocketAddress, DistributedInstance> knownInstances;
	
	/**
	 * The blacklisted instances, no connection will be created to these systems
	 */
	protected Set<InetSocketAddress> blacklist = new HashSet<>();
	
	/**
	 * Is the paging for queries enabled?
	 */
	protected boolean pagingEnabled;
	
	/**
	 * The amount of tuples per page
	 */
	protected short tuplesPerPage;
	
	/**
	 * The singleton instance
	 */
	protected static MembershipConnectionService instance = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MembershipConnectionService.class);
	
	private MembershipConnectionService() {
		final HashMap<InetSocketAddress, BBoxDBClient> connectionMap = new HashMap<InetSocketAddress, BBoxDBClient>();
		serverConnections = Collections.synchronizedMap(connectionMap);
		
		final HashMap<InetSocketAddress, DistributedInstance> instanceMap = new HashMap<InetSocketAddress, DistributedInstance>();
		knownInstances = Collections.synchronizedMap(instanceMap);
		
		pagingEnabled = false;
		tuplesPerPage = 0;
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
		DistributedInstanceManager.getInstance().registerListener(this);
		
		// Create connections to existing instances
		final List<DistributedInstance> instances = DistributedInstanceManager.getInstance().getInstances();
		
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
		DistributedInstanceManager.getInstance().removeListener(this);
		
		// Close all connections
		synchronized (serverConnections) {
			for(final InetSocketAddress instance : serverConnections.keySet()) {
				final BBoxDBClient client = serverConnections.get(instance);
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
		return "Mambership Connection Service";
	}
	
	/**
	 * Add a new connection to a bboxdb system
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
			logger.info("Not creating a connection to the blacklisted system: " + distributedInstance);
			return;
		}
		
		logger.info("Opening connection to instance: " + distributedInstance);
		
		final BBoxDBClient client = new BBoxDBClient(distributedInstance.getInetSocketAddress());
		client.setPagingEnabled(pagingEnabled);
		client.setTuplesPerPage(tuplesPerPage);
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
	 * Terminate the connection to a missing bboxdb system
	 * @param distributedInstance 
	 */
	protected synchronized void terminateConnection(final DistributedInstance distributedInstance) {
		
		if(! serverConnections.containsKey(distributedInstance.getInetSocketAddress())) {
			return;
		}
		
		logger.info("Closing connection to dead instance: " + distributedInstance);
		
		knownInstances.remove(distributedInstance.getInetSocketAddress());
		final BBoxDBClient client = serverConnections.remove(distributedInstance.getInetSocketAddress());
		client.terminateConnection();
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
	public BBoxDBClient getConnectionForInstance(final DistributedInstance instance) {
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
	public List<BBoxDBClient> getAllConnections() {
		return new ArrayList<BBoxDBClient>(serverConnections.values());
	}
	
	/**
	 * Get a list with all distributed instances we have connections to
	 * @return
	 */
	public List<DistributedInstance> getAllInstances() {
		return new ArrayList<DistributedInstance>(knownInstances.values());
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
		serverConnections.values().forEach(c -> c.setPagingEnabled(pagingEnabled));
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
		serverConnections.values().forEach(c -> c.setTuplesPerPage(tuplesPerPage));
	}
	
}

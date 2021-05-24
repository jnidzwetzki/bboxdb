/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.network.server.connection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bboxdb.commons.service.ServiceState;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
import org.bboxdb.distribution.region.GlobalDistributionRegionEventBus;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.client.ContinuousQueryServerStateFuture;
import org.bboxdb.network.entity.ContinuousQueryServerState;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.bboxdb.network.server.query.ClientQuery;
import org.bboxdb.network.server.query.continuous.ContinuousClientQuery;
import org.bboxdb.network.server.query.continuous.ContinuousQueryExecutionState;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkConnectionService implements BBoxDBService {
	
	/**
	 * The configuration
	 */
	private final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();

	/**
	 * Our thread pool to handle connections
	 */
	private ExecutorService threadPool;
	
	/**
	 * The connection handler state
	 */
	private final ServiceState state;
	
	/**
	 * The connection dispatcher runnable
	 */
	private ConnectionDispatcherRunable serverSocketDispatcher;
	
	/**
	 * The thread that listens on the server socket and dispatches
	 * incoming requests to the thread pool
	 */
	private Thread serverSocketDispatchThread;
	
	/**
	 * The storage reference
	 */
	private final TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The lock manager
	 */
	private final LockManager lockManager;
	
	/**
	 * The client connection registry
	 */
	private final ClientConnectionRegistry clientConnectionRegistry;
	
	private final DistributionRegionCallback callback = (e, r) -> handleCallback(e, r);
	
	/**
	 * The Logger
	 */
	final static Logger logger = LoggerFactory.getLogger(NetworkConnectionService.class);
	
	public NetworkConnectionService(final TupleStoreManagerRegistry storageRegistry) {
		this.storageRegistry = storageRegistry;
		this.state = new ServiceState();
		this.lockManager = new LockManager();
		this.clientConnectionRegistry = new ClientConnectionRegistry();
	}


	/**
	 * Start the network connection handler
	 */
	public void init() {
		
		if(! state.isInNewState()) {
			logger.info("init() called on ready instance, ignoring: {}", state.getState());
			return;
		}
		
		try {
			state.dipatchToStarting();
			
			final int port = configuration.getNetworkListenPort();
			logger.info("Start the network connection handler on port: {}", port);
			
			if(threadPool == null) {
				threadPool = Executors.newFixedThreadPool(configuration.getNetworkConnectionThreads());
			}
						
			serverSocketDispatcher = new ConnectionDispatcherRunable(port, threadPool, 
					storageRegistry, lockManager, clientConnectionRegistry);
			
			serverSocketDispatchThread = new Thread(serverSocketDispatcher);
			serverSocketDispatchThread.start();
			serverSocketDispatchThread.setName("Connection dispatcher thread");
			
			GlobalDistributionRegionEventBus.getInstance().registerCallback(callback);
						
			state.dispatchToRunning();
		} catch(Exception e) {
			logger.error("Got exception, setting state to failed", e);
			state.dispatchToFailed(e);
			shutdown();
		}
	}
	
	/**
	 * Shutdown the network connection
	 */
	public synchronized void shutdown() {
		
		if(! state.isInRunningState()) {
			logger.info("shutdown() called non running instance, ignoring: {}", state.getState());
			return;
		}
		
		logger.info("Shutdown the network connection handler");
		state.dispatchToStopping();
		
		if(serverSocketDispatchThread != null) {
			serverSocketDispatcher.closeSocketNE();
			serverSocketDispatchThread.interrupt();	
			serverSocketDispatchThread = null;
			serverSocketDispatcher = null;
		}
		
		if(threadPool != null) {
			threadPool.shutdown();
			threadPool = null;
		}
		
		GlobalDistributionRegionEventBus.getInstance().removeCallback(callback);

		state.dispatchToTerminated();
	}
	
	/**
	 * Handle the region callbacks (e.g., transfer the continuous query state)
	 * @param event
	 * @param region
	 */
	private void handleCallback(final DistributionRegionEvent event, final DistributionRegion region) {
		if(event != DistributionRegionEvent.LOCAL_MAPPING_ADDED) {
			return;
		}
		
		try {
			final Set<String> knownTables = new HashSet<>();
			
			// Query UUID -> State
			final Map<String, ContinuousQueryExecutionState> queryMap = new HashMap<>();
			final Set<ClientConnectionHandler> allConnections = clientConnectionRegistry.getAllActiveConnections();
			
			for(final ClientConnectionHandler connection : allConnections) {
				final Map<Short, ClientQuery> activeQueries = connection.getActiveQueries();
				
				for(final ClientQuery clientQuery : activeQueries.values()) {
					if(! (clientQuery instanceof ContinuousClientQuery)) {
						continue;
					}
					
					final ContinuousClientQuery continuousQuery = (ContinuousClientQuery) clientQuery;
					final ContinuousQueryPlan queryPlan = continuousQuery.getQueryPlan();
					
					final String streamTable = queryPlan.getStreamTable();
					final TupleStoreName tupleStoreName = new TupleStoreName(streamTable);
					if(tupleStoreName.getDistributionGroup().equals(region.getDistributionGroupName())) {
						queryMap.put(queryPlan.getQueryUUID(), continuousQuery.getContinuousQueryState());
						knownTables.add(queryPlan.getStreamTable());
					}
				}
			}
			
			final Set<BBoxDBInstance> systemsToRequst = getSystemsToConnectForStateSync(region);
			performStateSync(knownTables, queryMap, systemsToRequst);
		} catch (InterruptedException e) {
			logger.debug("Got interrupted exception", e);
			Thread.currentThread().interrupt();
		}
		
	}


	/**
	 * Perform the state sync of the continous queries
	 * @param knownTables
	 * @param queryMap
	 * @param systemsToRequst
	 * @throws InterruptedException
	 */
	private void performStateSync(final Set<String> knownTables,
			final Map<String, ContinuousQueryExecutionState> queryMap, final Set<BBoxDBInstance> systemsToRequst)
			throws InterruptedException {
		
		for(final String table : knownTables) {
			for(final BBoxDBInstance instance : systemsToRequst) {
				final BBoxDBConnection connection = MembershipConnectionService.getInstance().getConnectionForInstance(instance);
				final BBoxDBClient client = connection.getBboxDBClient();
				final TupleStoreName tupleStore = new TupleStoreName(table);
				final ContinuousQueryServerStateFuture resultFuture = client.getContinuousQueryState(tupleStore);
				
				resultFuture.waitForCompletion();
				
				if(resultFuture.isFailed()) {
					logger.error("The result future is failed", resultFuture.getAllMessages());
					continue;
				}
				
				final ContinuousQueryServerState resultState = resultFuture.get(0);
				
				final Map<String, Set<String>> rangeQueryState = resultState.getGlobalActiveRangeQueryElements();
				final Map<String, Map<String, Set<String>>> joinQueryState = resultState.getGlobalActiveJoinElements();
				
				for(final String queryUUID : resultState.getAllQueryIDs()) {
					final ContinuousQueryExecutionState localState = queryMap.get(queryUUID);
					final Set<String> rangeQueryStateForUUID = rangeQueryState.get(queryUUID);
					final Map<String, Set<String>> joinQueryStateForUUID = joinQueryState.get(queryUUID);
					
					localState.merge(rangeQueryStateForUUID, joinQueryStateForUUID);
				}
			}
		}
	}

	/**
	 * Determine which systems needs to be connected for the state sync
	 * of the continuous queries
	 * @param region
	 * @return
	 */
	private Set<BBoxDBInstance> getSystemsToConnectForStateSync(final DistributionRegion region) {
		final Set<BBoxDBInstance> systemsToRequst = new HashSet<>();
		
		if(region.isLeafRegion()) {
			// Split
			systemsToRequst.add(region.getParent().getSystems().get(0));
		} else {
			// Merge
			region.getAllChildren().forEach(r -> systemsToRequst.add(r.getSystems().get(0)));
		}
		return systemsToRequst;
	}
	
	@Override
	public String getServicename() {
		return "Network connection handler";
	}
}
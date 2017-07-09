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
package org.bboxdb.network.routing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.util.concurrent.ExceptionSafeThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageRouter {
	
	/**
	 * The thread pool
	 */
	protected final ExecutorService threadPool;
	
	/**
	 * The client connection handler
	 */
	protected final ClientConnectionHandler clientConnectionHandler;
	
	/**
	 * Amount of retries to route a package 
	 */
	public final static short ROUTING_RETRY = 3;
	
	/**
	 * Routing timeout
	 */
	protected final int ROUTING_TIMEOUT_IN_SEC = 10;
	
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PackageRouter.class);

	public PackageRouter(final ExecutorService threadPool, 
			final ClientConnectionHandler clientConnectionHandler) {
		
		this.threadPool = threadPool;
		this.clientConnectionHandler = clientConnectionHandler;
	}

	/**
	 * Perform the routing task async
	 * @param packageSequence
	 * @param insertTupleRequest
	 * @param boundingBox
	 */
	public void performInsertPackageRoutingAsync(final short packageSequence, 
			final InsertTupleRequest insertTupleRequest) {
	
		final Runnable routeRunable = new ExceptionSafeThread()  {

			@Override
			protected void runThread() {
				
				try {
					assert (insertTupleRequest.getRoutingHeader().isRoutedPackage()) : "Tuple is not a routed package";
					
					insertTupleRequest.getRoutingHeader().dispatchToNextHop();				
					final boolean routeResult = sendInsertPackage(insertTupleRequest);
	
					if(routeResult) {
						final SuccessResponse responsePackage = new SuccessResponse(packageSequence);
						clientConnectionHandler.writeResultPackage(responsePackage);
						return;
					} 
					
				}  catch(InterruptedException e) {
					logger.error("Exception while routing package", e);
					Thread.currentThread().interrupt();
				} catch (IOException | PackageEncodeException e) {
					logger.error("Exception while routing package", e);
				} 
				
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_ROUTING_FAILED);
				clientConnectionHandler.writeResultPackageNE(responsePackage);
			}
		};
		
		// Submit the runnable to our pool
		if(threadPool.isShutdown()) {
			logger.warn("Thread pool is shutting down, don't route package: {}", packageSequence);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_SHUTDOWN);
			clientConnectionHandler.writeResultPackageNE(responsePackage);
		} else {
			threadPool.submit(routeRunable);
		}
	}

	/**
	 * @param insertTupleRequest
	 * @return
	 * @throws InterruptedException
	 * @throws TimeoutException 
	 * @throws ExecutionException
	 */
	protected boolean sendInsertPackage(final InsertTupleRequest insertTupleRequest) 
			throws InterruptedException {
		
		final RoutingHeader routingHeader = insertTupleRequest.getRoutingHeader();
		
		if(routingHeader.reachedFinalInstance()) {
			return true;
		} 
		
		final RoutingHop routingHop = routingHeader.getRoutingHop();
		final DistributedInstance receiverInstance = routingHop.getDistributedInstance();
		
		final BBoxDBClient connection = MembershipConnectionService
				.getInstance()
				.getConnectionForInstance(receiverInstance);
		
		if(connection == null) {
			logger.error("Unable to get a connection to system: {}", receiverInstance);
			return false;
		} 
		
		final EmptyResultFuture insertFuture = connection.insertTuple(insertTupleRequest);
		
		try {
			insertFuture.waitForAll(ROUTING_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			logger.warn("Routing timeout, retry routing: {}", connection);
			return false;
		}
		
		final boolean operationSuccess = (! insertFuture.isFailed());
		return operationSuccess;
	}
	
	/**
	 * Handle a unrouted insert package
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws TimeoutException 
	 */
	protected boolean makePackageRoutable(final InsertTupleRequest insertTupleRequest, 
			final BoundingBox boundingBox) throws ZookeeperException, InterruptedException {
		
		int tryCounter = 0;
		
		while(tryCounter < PackageRouter.ROUTING_RETRY) {
			tryCounter++;
			
			// Routed package: Create routing list and route package
			final Set<DistributionRegion> destinationRegions = getDestinationRegions(insertTupleRequest, 
					boundingBox);
			
			setInsertRoutingHeader(insertTupleRequest, destinationRegions);
	
			final boolean sendResult = sendInsertPackage(insertTupleRequest);
			
			if(sendResult == true) {
				break;
			}
			
			logger.warn("Unable to send insert package, retry routing");
			
			// Wait some time to let the region assignment settle
			Thread.sleep(10);			
		}
		
		logger.warn("Unable to route package with {} retries", tryCounter);
		return false;
	}
	
	/**
	 * Prepare the routing header for the next hop
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return 
	 * @throws ZookeeperException
	 */
	protected void setInsertRoutingHeader(final InsertTupleRequest insertTupleRequest, 
			final Set<DistributionRegion> destinationRegions) throws ZookeeperException {
		
		final Map<DistributedInstance, RoutingHop> hops = new HashMap<>();
		for(final DistributionRegion region : destinationRegions) {
			final Collection<DistributedInstance> systems = region.getSystems();
			
			for(DistributedInstance system : systems) {
				if(! hops.containsKey(system)) {
					final List<Integer> regions = new ArrayList<>();
					regions.add(region.getRegionId());
					hops.put(system, new RoutingHop(system, regions));
				} else {
					final RoutingHop routingHop = hops.get(system);
					routingHop.addRegion(region.getRegionId());
				}
			}
		}
		
		final List<RoutingHop> hopList = new ArrayList<>(hops.values());
		
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 0, hopList);
		insertTupleRequest.replaceRoutingHeader(routingHeader);
	}

	/**
	 * Get routing destinations
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return
	 * @throws ZookeeperException
	 */
	protected Set<DistributionRegion> getDestinationRegions(
			final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) 
					throws ZookeeperException {
		
		final String distributionGroup = insertTupleRequest.getTable().getDistributionGroup();
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		
		final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForGroupName(
				distributionGroup, zookeeperClient);

		final DistributionRegion distributionRegion = distributionAdapter.getRootNode();
		
		return distributionRegion.getDistributionRegionsForBoundingBox(boundingBox);
	}
}

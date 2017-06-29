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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfigurationManager;
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
			final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) {
	
		final Runnable routeRunable = new ExceptionSafeThread()  {

			@Override
			protected void runThread() {
				
				try {
					final boolean routeResult 
						= routeInsertPackage(packageSequence, insertTupleRequest, boundingBox);
					
					if(routeResult) {
						final SuccessResponse responsePackage = new SuccessResponse(packageSequence);
						clientConnectionHandler.writeResultPackage(responsePackage);
						return;
					} 
					
				}  catch(InterruptedException e) {
					logger.error("Exception while routing package", e);
					Thread.currentThread().interrupt();
				} catch (ZookeeperException | IOException | PackageEncodeException e) {
					logger.error("Exception while routing package", e);
				} 
				
				final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_ROUTING_FAILED);
				clientConnectionHandler.writeResultPackageNE(responsePackage);
			}
		};
		
		// Submit the runnable to our pool
		if(threadPool.isShutdown()) {
			logger.warn("Thread pool is shutting down, don't route package: " + packageSequence);
			final ErrorResponse responsePackage = new ErrorResponse(packageSequence, ErrorMessages.ERROR_QUERY_SHUTDOWN);
			clientConnectionHandler.writeResultPackageNE(responsePackage);
		} else {
			threadPool.submit(routeRunable);
		}
	}

	/**
	 * Route the package to the next hop
	 * @param packageSequence
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	protected boolean routeInsertPackage(final short packageSequence, final InsertTupleRequest insertTupleRequest,
			final BoundingBox boundingBox) throws ZookeeperException, InterruptedException {
		
		if(insertTupleRequest.getRoutingHeader().isRoutedPackage()) {
			// Routed package: dispatch to next hop
			insertTupleRequest.getRoutingHeader().dispatchToNextHop();
			
			return sendInsertPackage(insertTupleRequest);
		} else {
			return handleUnroutedPackage(insertTupleRequest, boundingBox);
		}
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
	protected boolean handleUnroutedPackage(final InsertTupleRequest insertTupleRequest, 
			final BoundingBox boundingBox) throws ZookeeperException, InterruptedException {
		
		int tryCounter = 0;
		
		while(tryCounter < ROUTING_RETRY) {
			tryCounter++;
			
			// Unrouted package: Create routing list and route package
			final Set<DistributionRegion> instancesBeforeRouting = getRoutingDestinations(insertTupleRequest, boundingBox);
			final List<DistributedInstance> systemList = convertRegionsToDistributedInstances(instancesBeforeRouting);
			setInsertRoutingHeader(insertTupleRequest, systemList);
			
	
			final boolean sendResult = sendInsertPackage(insertTupleRequest);
			
			if(sendResult == false) {
				logger.warn("Unable to send insert package, retry routing");
				continue;
			}
			
			final Set<DistributionRegion> instancesAfterRouting = getRoutingDestinations(insertTupleRequest, boundingBox);

			if(instancesBeforeRouting.equals(instancesAfterRouting)) {
				return true;
			}
			
			logger.debug("Instance list differs before and after routing, retry package routing");
		}
		
		logger.warn("Unable to route package with " + tryCounter + " retries");
		return false;
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
		
		if(insertTupleRequest.getRoutingHeader().reachedFinalInstance()) {
			return true;
		} 
		
		final DistributedInstance receiver = insertTupleRequest.getRoutingHeader().getHopInstance();
		final BBoxDBClient connection = MembershipConnectionService.getInstance().getConnectionForInstance(receiver);
		
		if(connection == null) {
			logger.error("Unable to get a connection to system: {}", receiver);
			return false;
		} 
		
		final EmptyResultFuture insertFuture = connection.insertTuple(insertTupleRequest);
		
		try {
			insertFuture.waitForAll(ROUTING_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			logger.warn("Routing timeout, retry routing: {}", connection);
			return false;
		}
		
		final boolean success = ! insertFuture.isFailed();
		return success;
	}
	
	/**
	 * Prepare the routing header for the next hop
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return 
	 * @throws ZookeeperException
	 */
	protected void setInsertRoutingHeader(final InsertTupleRequest insertTupleRequest, 
			final List<DistributedInstance> systems) throws ZookeeperException {
		
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 0, systems);
		insertTupleRequest.replaceRoutingHeader(routingHeader);
	}

	/**
	 * Get routing destinations
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return
	 * @throws ZookeeperException
	 */
	protected Set<DistributionRegion> getRoutingDestinations(
			final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) 
					throws ZookeeperException {
		
		final String distributionGroup = insertTupleRequest.getTable().getDistributionGroup();
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		
		final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForGroupName(
				distributionGroup, zookeeperClient);

		final DistributionRegion distributionRegion = distributionAdapter.getRootNode();
		
		return distributionRegion.getDistributionRegionsForBoundingBox(boundingBox);
	}
	
	
	/**
	 * Convert the region list into a system list
	 * @param regions
	 * @return
	 */
	protected List<DistributedInstance> convertRegionsToDistributedInstances(
			final Set<DistributionRegion> regions) {
		
		final List<DistributedInstance> systems = new ArrayList<DistributedInstance>();

		for(final DistributionRegion region : regions) {
			for(DistributedInstance instance : region.getSystems()) {
				if(! systems.contains(instance)) {
					systems.add(instance);
				}
			}
		}
		
		// Remove the local instance
		final DistributedInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName(BBoxDBConfigurationManager.getConfiguration());
		
		final List<DistributedInstance> systemsWithoutLocalInstance = systems
			.stream()
			.filter(i -> ! i.getInetSocketAddress().equals(localInstanceName.getInetSocketAddress()))
			.collect(Collectors.toList());

		return systemsWithoutLocalInstance;
	}
}

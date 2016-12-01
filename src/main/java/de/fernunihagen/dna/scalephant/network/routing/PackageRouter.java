/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.network.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.scalephant.network.client.future.ClientOperationFuture;
import de.fernunihagen.dna.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.scalephant.network.packages.response.ErrorResponse;
import de.fernunihagen.dna.scalephant.network.packages.response.SuccessResponse;
import de.fernunihagen.dna.scalephant.network.server.ClientConnectionHandler;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;

public class PackageRouter {
	
	/**
	 * The thread pool
	 */
	protected final ThreadPoolExecutor threadPool;
	
	/**
	 * The client connection handler
	 */
	protected final ClientConnectionHandler clientConnectionHandler;
	
	/**
	 * Amount of retries to route a package 
	 */
	public final static short ROUTING_RETRY = 3;
	
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PackageRouter.class);

	public PackageRouter(final ThreadPoolExecutor threadPool, final ClientConnectionHandler clientConnectionHandler) {
		this.threadPool = threadPool;
		this.clientConnectionHandler = clientConnectionHandler;
	}

	/**
	 * Perform the routing task async
	 * @param packageSequence
	 * @param insertTupleRequest
	 * @param boundingBox
	 */
	public void performInsertPackageRoutingAsync(final short packageSequence, final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) {
	
		final Runnable routeRunable = new Runnable() {
			@Override
			public void run() {
				boolean routeResult;
				try {
					routeResult = routeInsertPackage(packageSequence, insertTupleRequest, boundingBox);
					
					if(routeResult) {
						clientConnectionHandler.writeResultPackage(new SuccessResponse(packageSequence));
					} else {
						clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence));
					}
				} catch (ZookeeperException | InterruptedException | ExecutionException e) {
					logger.warn("Exception while routing package", e);
				}
			}
		};
		
		// Submit the runnable to our pool
		if(threadPool.isTerminating()) {
			logger.warn("Thread pool is shutting down, don't route package: " + packageSequence);
			clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence));
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
	 * @throws ExecutionException
	 */
	protected boolean routeInsertPackage(final short packageSequence, final InsertTupleRequest insertTupleRequest,
			final BoundingBox boundingBox) throws ZookeeperException, InterruptedException, ExecutionException {
		
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
	 * @throws ExecutionException
	 */
	protected boolean handleUnroutedPackage(final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) throws ZookeeperException, InterruptedException, ExecutionException {
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
	 * @throws ExecutionException
	 */
	protected boolean sendInsertPackage(final InsertTupleRequest insertTupleRequest) throws InterruptedException, ExecutionException {
		if(insertTupleRequest.getRoutingHeader().reachedFinalInstance()) {
			return true;
		} 
		
		final DistributedInstance receiver = insertTupleRequest.getRoutingHeader().getHopInstance();
		final ScalephantClient connection = MembershipConnectionService.getInstance().getConnectionForInstance(receiver);
		
		if(connection == null) {
			logger.error("Unable to get a connection to system: " + receiver);
			return false;
		} 
		
		final ClientOperationFuture insertFuture = connection.insertTuple(insertTupleRequest);
		insertFuture.waitForAll();
		
		if(insertFuture.isFailed()) {
			return false;
		} else {
			return true;
		}
	}
	
	/**
	 * Prepare the routing header for the next hop
	 * @param insertTupleRequest
	 * @param boundingBox
	 * @return 
	 * @throws ZookeeperException
	 */
	protected void setInsertRoutingHeader(final InsertTupleRequest insertTupleRequest, final List<DistributedInstance> systems) throws ZookeeperException {
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
	protected Set<DistributionRegion> getRoutingDestinations(final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) throws ZookeeperException {
		
		final String distributionGroup = insertTupleRequest.getTable().getDistributionGroup();
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForGroupName(distributionGroup, zookeeperClient);
		
		final Set<DistributionRegion> regions = distributionRegion.getDistributionRegionsForBoundingBox(boundingBox);
		
		return regions;
	}
	
	
	/**
	 * Convert the region list into a system list
	 * @param regions
	 * @return
	 */
	protected List<DistributedInstance> convertRegionsToDistributedInstances(final Set<DistributionRegion> regions) {
		final List<DistributedInstance> systems = new ArrayList<DistributedInstance>();

		for(final DistributionRegion region : regions) {
			for(DistributedInstance instance : region.getSystems()) {
				if(! systems.contains(instance)) {
					systems.add(instance);
				}
			}
		}
		
		// Remove the local instance
		final DistributedInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName(ScalephantConfigurationManager.getConfiguration());
		systems.remove(localInstanceName);
		
		return systems;
	}
}

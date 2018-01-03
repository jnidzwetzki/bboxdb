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
package org.bboxdb.network.routing;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.server.ClientConnectionHandler;
import org.bboxdb.network.server.ErrorMessages;
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
		final BBoxDBInstance receiverInstance = routingHop.getDistributedInstance();
		
		final BBoxDBClient connection = MembershipConnectionService
				.getInstance()
				.getConnectionForInstance(receiverInstance);
		
		if(connection == null) {
			logger.error("Unable to get a connection to system: {}", receiverInstance);
			return false;
		} 
		
		final EmptyResultFuture insertFuture = connection.insertTuple(
				insertTupleRequest.getTable().getFullname(), 
				insertTupleRequest.getTuple(), 
				routingHeader);
		
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
	 * Ensure that the package is routed to the right system
	 * @param localHop
	 * @throws BBoxDBException
	 */
	public static void checkLocalSystemNameMatches(final RoutingHop localHop) throws BBoxDBException {
		final BBoxDBInstance localInstanceName = ZookeeperClientFactory.getLocalInstanceName();
		final BBoxDBInstance routingInstanceName = localHop.getDistributedInstance();
		
		if(! localInstanceName.socketAddressEquals(routingInstanceName)) {
			throw new BBoxDBException("Routing hop " + routingInstanceName 
					+ " does not match local host " + localInstanceName);
		}
	}
}

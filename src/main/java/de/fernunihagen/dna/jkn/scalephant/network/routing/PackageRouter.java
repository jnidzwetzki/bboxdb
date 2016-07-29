package de.fernunihagen.dna.jkn.scalephant.network.routing;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClientFactory;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.jkn.scalephant.network.client.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.ErrorResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessResponse;
import de.fernunihagen.dna.jkn.scalephant.network.server.ClientConnectionHandler;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

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
		
		// Create a new routing header or dispatch to next system
		prepareRountingHeader(insertTupleRequest, boundingBox);
		
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
	 * @throws ZookeeperException
	 */
	protected boolean prepareRountingHeader(final InsertTupleRequest insertTupleRequest, final BoundingBox boundingBox) throws ZookeeperException {
		if(! insertTupleRequest.getRoutingHeader().isRoutedPackage()) {
			// Unrouted package: Create routing list
			final String distributionGroup = insertTupleRequest.getTable().getDistributionGroup();
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final DistributionRegion distributionRegion = DistributionGroupCache.getGroupForGroupName(distributionGroup, zookeeperClient);
			final List<DistributedInstance> instances = distributionRegion.getSystemsForBoundingBox(boundingBox);
			
			// Remove the local instance
			instances.remove(ZookeeperClientFactory.getLocalInstanceName(ScalephantConfigurationManager.getConfiguration()));
			
			final RoutingHeader routingHeader = new RoutingHeader(true, (short) 0, instances);
			insertTupleRequest.replaceRoutingHeader(routingHeader);
			
			// New routing header created
			return true;
		} else { 
			// Routed package: dispatch to next hop
			insertTupleRequest.getRoutingHeader().dispatchToNextHop();

			// No new routing header created
			return false;
		}
	}
}

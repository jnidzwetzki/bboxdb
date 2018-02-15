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
package org.bboxdb.network.client;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkOperationRetryer implements Closeable {

	/**
	 * The pending packages
	 */
	private final Map<Short, RetryPackageEntity> packages = new HashMap<>();
	
	/**
	 * The retry consumer
	 */
	private final BiConsumer<NetworkRequestPackage, OperationFuture> retryConsumer;
	
	/**
	 * The tuple send delayer
	 */
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkOperationRetryer.class);

	public NetworkOperationRetryer(final BiConsumer<NetworkRequestPackage, OperationFuture> retryConsumer) {
		this.retryConsumer = retryConsumer;
	}

	/**
	 * Register an operation for try
	 * @param packageId
	 * @param networkPackage
	 */
	public void registerOperation(final short packageId, final NetworkRequestPackage networkPackage, 
			final OperationFuture future) {
		
		final Short packageIdShort = Short.valueOf(packageId);

		if(isPackageIdKnown(packageIdShort)) {
			throw new IllegalArgumentException("Package is already known: " + packageId);
		}
		
		packages.put(packageIdShort, new RetryPackageEntity(networkPackage, future));
	}

	/**
	 * Is the package id known
	 * @param packageIdShort
	 * @return
	 */
	public boolean isPackageIdKnown(final Short packageIdShort) {
		return packages.containsKey(packageIdShort);
	}
	
	/**
	 * Handle operation success
	 * @param packageId
	 */
	public void handleSuccess(final short packageId) {
		packages.remove(packageId);
	}
	
	/**
	 * Handle operation failure
	 * @param packageId
	 * @param errorMessage 
	 * @return
	 */
	public boolean handleFailure(final short packageId, final String errorMessage) {
		final Short packageIdShort = Short.valueOf(packageId);
		
		if(! isPackageIdKnown(packageIdShort)) {
			throw new IllegalArgumentException("Package id is not known: " + packageId);
		}
		
		final RetryPackageEntity retryPackageEntity = packages.get(packageIdShort);
		
		if(canPackageRetried(retryPackageEntity)) {
			retryPackageEntity.increaseRetryCounter();
			final OperationFuture future = retryPackageEntity.getFuture();
			final NetworkRequestPackage networkPackage = retryPackageEntity.getNetworkPackage();
			
			logger.debug("Got failed package but retry: {} (id: {})", errorMessage, packageId);

			final Runnable futureTask = () -> {
				retryConsumer.accept(networkPackage, future);
			};
			
			// Wait some time to let the global index change
			final int delay = 100 * retryPackageEntity.getRetryCounter();
			scheduler.schedule(futureTask, delay, TimeUnit.MILLISECONDS);
			
			return true;
		} else {
			// Retry failed, remove package
			packages.remove(packageIdShort);
			return false;
		}
	}

	/**
	 * Can the package operation be retried on failure?
	 * 
	 * @param retryPackageEntity
	 * @return
	 */
	private boolean canPackageRetried(final RetryPackageEntity retryPackageEntity) {
		
		if(! retryPackageEntity.getNetworkPackage().canBeRetriedOnFailure()) {
			return false;
		}
		
		return retryPackageEntity.getRetryCounter() < Const.OPERATION_RETRY;
	}
	
	/**
	 * Clear all known packages
	 */
	@Override
	public void close() {
		packages.clear();
		scheduler.shutdown();
	}
}

class RetryPackageEntity {
	
	/**
	 * The network package
	 */
	protected NetworkRequestPackage networkPackage;
	
	/**
	 * The amount of retrys
	 */
	protected short retryCounter;
	
	/**
	 * The future
	 */
	protected OperationFuture future;
	
	public RetryPackageEntity(final NetworkRequestPackage networkPackage, OperationFuture future) {
		this.networkPackage = networkPackage;
		this.future = future;
		this.retryCounter = 0;
	}
	
	public short getRetryCounter() {
		return retryCounter;
	}
	
	public NetworkRequestPackage getNetworkPackage() {
		return networkPackage;
	}	
	
	public void increaseRetryCounter() {
		retryCounter++;
	}
	
	public OperationFuture getFuture() {
		return future;
	}
}
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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;

public class NetworkOperationRetryer {
	
	/**
	 * The pending packages
	 */
	protected final Map<Short, RetryPackageEntity> packages = new HashMap<>();
	
	/**
	 * The retry consumer
	 */
	protected final BiConsumer<NetworkRequestPackage, OperationFuture> retryConsumer;

	public NetworkOperationRetryer(final BiConsumer<NetworkRequestPackage, OperationFuture> retryConsumer) {
		this.retryConsumer = retryConsumer;
	}

	/**
	 * Regiter an operation for try
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
	 * @return
	 */
	public boolean handleFailure(final short packageId) {
		final Short packageIdShort = Short.valueOf(packageId);
		
		if(! isPackageIdKnown(packageIdShort)) {
			throw new IllegalArgumentException("Package is now known: " + packageId);
		}
		
		final RetryPackageEntity retryPackageEntity = packages.get(packageIdShort);
		
		if(retryPackageEntity.getRetryCounter() < Const.OPERATION_RETRY) {
			retryPackageEntity.increaseRetryCounter();
			retryConsumer.accept(retryPackageEntity.getNetworkPackage(), retryPackageEntity.getFuture());
			return true;
		} else {
			// Retry failed, remove
			packages.remove(packageIdShort);
			return false;
		}
	}
	
	/**
	 * Clear all known packages
	 */
	public void clear() {
		packages.clear();
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
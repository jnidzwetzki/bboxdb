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
package org.bboxdb.network.client;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.packages.NetworkRequestPackage;

public class NetworkOperationRetryer {
	
	/**
	 * The pending packages
	 */
	protected final Map<Short, RetryPackageEntity> packages = new HashMap<>();
	
	protected final Consumer<NetworkRequestPackage> retryConsumer;

	public NetworkOperationRetryer(final Consumer<NetworkRequestPackage> retryConsumer) {
		this.retryConsumer = retryConsumer;
	}

	/**
	 * Regiter an operation for try
	 * @param packageId
	 * @param networkPackage
	 */
	public void registerOperation(final short packageId, final NetworkRequestPackage networkPackage) {
		final Short packageIdShort = Short.valueOf(packageId);

		if(packages.containsKey(packageIdShort)) {
			throw new IllegalArgumentException("Package is already known: " + packageId);
		}
		
		packages.put(packageIdShort, new RetryPackageEntity(networkPackage));
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
		
		if(! packages.containsKey(packageIdShort)) {
			throw new IllegalArgumentException("Package is now known: " + packageId);
		}
		
		final RetryPackageEntity retryPackageEntity = packages.get(packageIdShort);
		
		if(retryPackageEntity.getRetryCounter() < Const.OPERATION_RETRY) {
			retryPackageEntity.increaseRetryCounter();
			retryConsumer.accept(retryPackageEntity.getNetworkPackage());
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
	
	public RetryPackageEntity(final NetworkRequestPackage networkPackage) {
		this.networkPackage = networkPackage;
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
}
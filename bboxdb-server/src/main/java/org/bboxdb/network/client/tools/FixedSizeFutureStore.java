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
package org.bboxdb.network.client.tools;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bboxdb.network.client.future.OperationFuture;

public class FixedSizeFutureStore {

	/**
	 * The amount of max pending futures
	 */
	protected final long maxPendingFutures;

	/**
	 * The pending futures list
	 */
	protected final List<OperationFuture> pendingFutures;
	
	/**
	 * The failed future callback
	 */
	protected final List<FailedFutureCallback> failedFutureCallbacks;
	
	public FixedSizeFutureStore(final long maxPendingFutures) {
		this.maxPendingFutures = maxPendingFutures;
		this.pendingFutures = new LinkedList<>();
		this.failedFutureCallbacks = new ArrayList<>();
	}

	/**
	 * Put a new future into the store
	 * 
	 * This method might block when to much futures are unfinished
	 */
	public void put(final OperationFuture futureToAdd) {
		pendingFutures.add(futureToAdd);

		checkAndCleanupRunningFuture();
	}

	/**
	 * Check and cleanup running futures
	 */
	protected void checkAndCleanupRunningFuture() {
		if (pendingFutures.size() <= maxPendingFutures) {
			return;
		}
		
		// Reduce futures
		while (isCleanupNeeded()) {

			removeCompleteFutures();

			// Still to much futures? Wait some time
			if (isCleanupNeeded()) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}

	/**
	 * Remove all complete futures
	 */
	protected void removeCompleteFutures() {
		
		// Handle failed futures
		pendingFutures.stream()
				.filter(f -> f.isFailed())
				.forEach(f -> handleFailedFuture(f));
		
		// Remove old futures
		pendingFutures.removeIf(f -> f.isDone());
	}

	/**
	 * Handle a failed future
	 * @param future
	 */
	protected void handleFailedFuture(final OperationFuture future) {		
		failedFutureCallbacks.forEach(c -> c.handleFailedFuture(future));
	}

	/**
	 * Is the future cleanup needed?
	 * @return
	 */
	protected boolean isCleanupNeeded() {
		return pendingFutures.size() > maxPendingFutures * 0.8;
	}
	
	/**
	 * The the max number of pending futures
	 * @return
	 */
	public long getMaxPendingFutures() {
		return maxPendingFutures;
	}
	
	/**
	 * Get the amount of pending futures
	 * @return
	 */
	public long getPendingFutureCount() {
		return pendingFutures.size();
	}
	
	/** 
	 * Add a new failed future callback
	 */
	public void addFailedFutureCallback(final FailedFutureCallback callback) {
		failedFutureCallbacks.add(callback);
	}
	
	/**
	 * Wait for the completion of all pending futures
	 * @throws InterruptedException 
	 * 
	 */
	public void waitForCompletion() throws InterruptedException {
		while(pendingFutures.size() > 0) {
			for(final OperationFuture future : pendingFutures) {
				future.waitForAll();
			}
			
			removeCompleteFutures();
		}
	}

}

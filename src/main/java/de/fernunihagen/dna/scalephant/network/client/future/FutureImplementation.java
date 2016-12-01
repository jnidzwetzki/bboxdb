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
package de.fernunihagen.dna.scalephant.network.client.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureImplementation {
	
	/**
	 * The id of the operation
	 */
	protected short requestId;
	
	/**
	 * The result of the operation
	 */
	protected volatile Object operationResult = null;
	
	/**
	 * The mutex for sync operations
	 */
	protected final Object mutex = new Object();
	
	/**
	 * The error flag for the operation
	 */
	protected volatile boolean failed = false;
	
	/**
	 * The done flag
	 */
	protected volatile boolean done = false;
	
	/**
	 * Empty constructor
	 */
	public FutureImplementation() {
	}
	
	/**
	 * Constructor with the request id
	 * @param requestId
	 */
	public FutureImplementation(final short requestId) {
		this.requestId = requestId;
	}

	public boolean isDone() {
		return done;
	}

	public Object get() throws InterruptedException, ExecutionException {
		
		synchronized (mutex) {
			while(! done) {
				mutex.wait();
			}
		}
		
		return operationResult;
	}

	public Object get(final long timeout, final TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		
		synchronized (mutex) {
			while(! done) {
				mutex.wait(unit.toMillis(timeout));
			}
		}
				
		return operationResult;
	}

	/**
	 * Returns the request id
	 */
	public short getRequestId() {
		return requestId;
	}

	/**
	 * Set the result of the operation
	 */
	public void setOperationResult(final Object result) {
		
		synchronized (mutex) {
			this.operationResult = result;
			mutex.notifyAll();
		}
	}

	/**
	 * Set the ID of the request
	 */
	public void setRequestId(final short requestId) {
		this.requestId = requestId;
	}

	/**
	 * Is the operation successful
	 * @return
	 */
	public boolean isFailed() {
		return failed;
	}

	/**
	 * Set the error flag for the operation
	 */
	public void setFailedState() {
		failed = true;
	}

	/**
	 * Wait for the completion of the future
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public boolean waitForCompletion() throws InterruptedException, ExecutionException {
		get();
		return true;
	}

	/**
	 * Fire the completion event
	 */
	public void fireCompleteEvent() {
		
		done = true;
		
		synchronized (mutex) {
			mutex.notifyAll();
		}
	}
}

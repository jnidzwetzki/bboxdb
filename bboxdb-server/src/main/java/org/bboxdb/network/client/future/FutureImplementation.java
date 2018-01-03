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
package org.bboxdb.network.client.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureImplementation<T> {
	
	/**
	 * The id of the operation
	 */
	protected short requestId;
	
	/**
	 * The result of the operation
	 */
	protected volatile T operationResult = null;
	
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
	 * Additional message
	 */
	protected String message;
	
	/**
	 * The future start time
	 */
	protected final long startTime;
	
	/**
	 * The future completion time
	 */
	protected long completionTime;
	
	/**
	 * The associated connection
	 */
	protected String connectionName;
	
	/**
	 * Empty constructor
	 */
	public FutureImplementation() {
		startTime = System.nanoTime();
	}
	
	/**
	 * Constructor with the request id
	 * @param requestId
	 */
	public FutureImplementation(final short requestId) {
		this();
		this.requestId = requestId;
	}

	/**
	 * Is the operation done?
	 * @return
	 */
	public boolean isDone() {
		return done;
	}

	/**
	 * Get (and wait) for the result
	 * @return
	 * @throws InterruptedException
	 */
	public T get() throws InterruptedException {
		
		synchronized (mutex) {
			while(! done) {
				mutex.wait();
			}
		}
		
		return operationResult;
	}

	/**
	 * Get (and wait) for the result
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @throws TimeoutException 
	 */
	public T get(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
		
		final long waitBegin = System.currentTimeMillis();
		final long waitTimeInMilis = unit.toMillis(timeout);

		synchronized (mutex) {
			while(! done) {
				mutex.wait(waitTimeInMilis);
				final long waitNow = System.currentTimeMillis();
				
				final long passedTime = waitNow - waitBegin;
				
				if(passedTime > waitTimeInMilis) {
					throw new TimeoutException("Unable to receive data in " + passedTime + " ms");
				}
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
	public void setOperationResult(final T result) {
		
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
	public boolean waitForCompletion() throws InterruptedException {
		get();
		return true;
	}

	/**
	 * Fire the completion event
	 */
	public void fireCompleteEvent() {
		
		done = true;
		completionTime = System.nanoTime();
		
		synchronized (mutex) {
			mutex.notifyAll();
		}
	}

	/**
	 * Get the message of the result
	 * @return
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * Set the message of the result
	 * @param message
	 */
	public void setMessage(final String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "FutureImplementation [requestId=" + requestId + ", operationResult=" + operationResult + ", mutex="
				+ mutex + ", failed=" + failed + ", done=" + done + ", message=" + message + ", startTime=" + startTime
				+ ", completionTime=" + completionTime + ", connectionId=" + connectionName + "]";
	}

	/**
	 * Get the elapsed time in nanoseconds for task completion
	 * @return
	 */
	public long getCompletionTime() {
		if (! isDone()) {
			throw new IllegalArgumentException("The future is not done. Unable to calculate completion time");
		}
		
		return completionTime - startTime;
	}

	/**
	 * Get the id of the connection
	 * @return
	 */
	public String getConnectionName() {
		return connectionName;
	}

	/**
	 * Set the id of the connection
	 * @param connectionName
	 */
	public void setConnectionName(final String connectionName) {
		this.connectionName = connectionName;
	}
	
	/**
	 * Get the message and the connection id in a human readable format
	 * @return
	 */
	public String getMessageWithConnectionName() {
		final StringBuilder sb = new StringBuilder();
		sb.append("[message=");
		sb.append(getMessage());
		sb.append(", connection=");
		sb.append(getConnectionName());
		sb.append("]");
		return sb.toString();
	}
	
}

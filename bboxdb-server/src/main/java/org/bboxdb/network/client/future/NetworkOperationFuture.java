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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class NetworkOperationFuture {
	
	/**
	 * The id of the operation
	 */
	private short requestId;
	
	/**
	 * The result of the operation
	 */
	private volatile Object operationResult = null;
	
	/**
	 * The latch for sync operations
	 */
	private final CountDownLatch latch = new CountDownLatch(1);
	
	/**
	 * The error flag for the operation
	 */
	private volatile boolean failed = false;
	
	/**
	 * The done flag
	 */
	private volatile boolean done = false;
	
	/**
	 * The complete / partial result flag
	 */
	private volatile boolean complete = true;
	
	/**
	 * Additional message
	 */
	private String message;
	
	/**
	 * The future start time
	 */
	private final Stopwatch stopwatch;
	
	/**
	 * The associated connection
	 */
	private final BBoxDBConnection connection;
	
	/**
	 * The package supplier
	 */
	private Supplier<NetworkRequestPackage> packageSupplier;

	/**
	 * The executions
	 */
	private final AtomicInteger executions = new AtomicInteger(0);
	
	/**
	 * The last send package
	 */
	private NetworkRequestPackage lastTransmittedPackage;
	
	/**
	 * The success callback
	 */
	protected Consumer<NetworkOperationFuture> successCallback;
	
	/**
	 * The error callback
	 */
	protected FutureErrorCallback errorCallback;

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(NetworkOperationFuture.class);

	/**
	 * Empty constructor
	 */
	public NetworkOperationFuture(final BBoxDBConnection connection, 
			final Supplier<NetworkRequestPackage> packageSupplier) {
		
		this.packageSupplier = packageSupplier;
		this.stopwatch = Stopwatch.createStarted();
		this.connection = connection;
	}

	/**
	 * Is the operation done?
	 * @return
	 */
	public boolean isDone() {
		return done;
	}

	/**
	 * Reexecute
	 */
	public void execute() {		
		this.lastTransmittedPackage = packageSupplier.get();
		this.failed = false;		
		this.executions.incrementAndGet();
		
		// Can be null in some unit tests
		if(lastTransmittedPackage != null) {
			this.requestId = lastTransmittedPackage.getSequenceNumber();
		}
		
		connection.registerPackageCallback(lastTransmittedPackage, this);
		connection.sendPackageToServer(lastTransmittedPackage, this);
	}
	
	/**
	 * Get (and wait) for the result
	 * @return
	 * @throws InterruptedException
	 */
	public Object get() throws InterruptedException {
		latch.await();
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
	public Object get(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {

		latch.await(timeout, unit);
		
		if(! done) {
			throw new TimeoutException("Unable to receive data in " + timeout + " " + unit);
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
		this.operationResult = result;
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
		
		// Is already be done
		if(done) {
			return;
		}
				
		// Run error handler
		if(errorCallback != null && failed) {
			final boolean couldBeHandled = errorCallback.handleError(this);
			if(couldBeHandled) {
				failed = false;
				return;
			}						
		}
				
		done = true;
		stopwatch.stop();
		latch.countDown();
		
		// Run success handler
		if(successCallback != null) {
			successCallback.accept(this);
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

	/**
	 * Is the given result complete?
	 * @return
	 */
	public boolean isCompleteResult() {
		return complete;
	}

	/**
	 * Set the complete flag
	 * @param complete
	 */
	public void setCompleteResult(final boolean complete) {
		this.complete = complete;
	}

	@Override
	public String toString() {
		return "NetworkOperationFuture [requestId=" + requestId + ", operationResult=" + operationResult + ", latch="
				+ latch + ", failed=" + failed + ", done=" + done + ", complete=" + complete + ", message=" + message
				+ ", stopwatch=" + stopwatch + ", connection=" + connection + "]";
	}

	/**
	 * Get the needed time for task completion
	 * @return
	 */
	public long getCompletionTime(final TimeUnit timeUnit) {
		if (! isDone()) {
			throw new IllegalArgumentException("The future is not done. Unable to calculate completion time");
		}
		
		return stopwatch.elapsed(timeUnit);
	}

	/**
	 * Get the id of the connection
	 * @return
	 */
	public BBoxDBConnection getConnection() {
		return connection;
	}
	
	/**
	 * The last transmitted package
	 * @return
	 */
	public NetworkRequestPackage getTransmittedPackage() {
		return lastTransmittedPackage;
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
		
		if(getConnection() == null) {
			sb.append("null");
		} else {
			sb.append(connection.getConnectionName());
		}
		
		sb.append("]");
		return sb.toString();
	}
	
	/**
	 * The error callback
	 * 
	 * @param errorCallback
	 */
	public void setErrorCallback(final FutureErrorCallback errorCallback) {
		this.errorCallback = errorCallback;
	}
	
	/**
	 * The success callback
	 * @param successCallback
	 */
	public void setSuccessCallback(final Consumer<NetworkOperationFuture> successCallback) {
		this.successCallback = successCallback;
	}
	
	/**
	 * Get the number of executions
	 * @return
	 */
	public int getExecutions() {
		return executions.get();
	}
	
}

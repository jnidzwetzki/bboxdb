/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.network.client.future.network;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.client.FutureErrorCallback;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.routing.RoutingHop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class NetworkOperationFutureImpl implements NetworkOperationFuture {

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
	 * The total number of retries before the future fails
	 */
	private int totalRetries = 50;

	/**
	 * The last send package
	 */
	private NetworkRequestPackage lastTransmittedPackage;

	/**
	 * The success callback
	 */
	protected Consumer<NetworkOperationFuture> doneCallback;

	/**
	 * The error callback
	 */
	protected FutureErrorCallback errorCallback;

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(NetworkOperationFutureImpl.class);
	
	/**
	 * The number of reties for operations that should fail fast
	 */
	public static final int FAST_FAIL_RETRIES = 3;

	/**
	 * Empty constructor
	 */
	public NetworkOperationFutureImpl(final BBoxDBConnection connection,
			final Supplier<NetworkRequestPackage> packageSupplier) {

		this.packageSupplier = packageSupplier;
		this.stopwatch = Stopwatch.createStarted();
		this.connection = connection;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#isDone()
	 */
	@Override
	public boolean isDone() {
		return done;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#execute()
	 */
	@Override
	public void execute() {
		
		if(done) {
			// e.g. replicated future
			logger.debug("Dont execute future because is already sunccessfully done");
			return;
		}

		final NetworkRequestPackage nextPackage = packageSupplier.get();
		
		if(connection.getConnectionState().isInFinishedState()) {
			final short packageId = (nextPackage != null) ? nextPackage.getSequenceNumber() : -1;
			final String packageClass = (nextPackage != null) ? nextPackage.getClass().toString() : "undefined";
			
			logger.error("Don't execute future because connection is closed: [connection={}, seq={}, package={}]", 
					connection.getConnectionState(), packageId, packageClass);
			
			setFailedState();
			fireCompleteEvent();
			return;
		}
		
		this.lastTransmittedPackage = nextPackage;
		this.failed = false;
		this.executions.incrementAndGet();

		// Can be null in some unit tests
		if(lastTransmittedPackage != null) {
			this.requestId = lastTransmittedPackage.getSequenceNumber();
		}
		
		connection.registerPackageCallback(lastTransmittedPackage, this);
		connection.sendPackageToServer(lastTransmittedPackage, this);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#get()
	 */
	@Override
	public Object get(final boolean waitForCompletion) throws InterruptedException {
		if(waitForCompletion) {
			latch.await();
		}
		
		return operationResult;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getRequestId()
	 */
	@Override
	public short getRequestId() {
		return requestId;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setOperationResult(java.lang.Object)
	 */
	@Override
	public void setOperationResult(final Object result) {
		this.operationResult = result;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#isFailed()
	 */
	@Override
	public boolean isFailed() {
		return failed;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setFailedState()
	 */
	@Override
	public void setFailedState() {
		failed = true;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#fireCompleteEvent()
	 */
	@Override
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

		// Run done handler
		if(doneCallback != null) {
			doneCallback.accept(this);
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getMessage()
	 */
	@Override
	public String getMessage() {
		return message;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setMessage(java.lang.String)
	 */
	@Override
	public void setMessage(final String message) {
		this.message = message;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#isCompleteResult()
	 */
	@Override
	public boolean isCompleteResult() {
		return complete;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setCompleteResult(boolean)
	 */
	@Override
	public void setCompleteResult(final boolean complete) {
		this.complete = complete;
	}

	@Override
	public String toString() {
		return "NetworkOperationFutureImpl [requestId=" + requestId + ", operationResult=" + operationResult + ", latch="
				+ latch + ", failed=" + failed + ", done=" + done + ", complete=" + complete + ", message=" + message
				+ ", stopwatch=" + stopwatch + ", connection=" + connection + "]";
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getCompletionTime(java.util.concurrent.TimeUnit)
	 */
	@Override
	public long getCompletionTime(final TimeUnit timeUnit) {
		if (! isDone()) {
			throw new IllegalArgumentException("The future is not done. Unable to calculate completion time");
		}

		return stopwatch.elapsed(timeUnit);
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getConnection()
	 */
	@Override
	public BBoxDBConnection getConnection() {
		return connection;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getTransmittedPackage()
	 */
	@Override
	public NetworkRequestPackage getTransmittedPackage() {
		return lastTransmittedPackage;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getMessageWithConnectionName()
	 */
	@Override
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

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setErrorCallback(org.bboxdb.network.client.future.FutureErrorCallback)
	 */
	@Override
	public void setErrorCallback(final FutureErrorCallback errorCallback) {
		this.errorCallback = errorCallback;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#setSuccessCallback(java.util.function.Consumer)
	 */
	@Override
	public void setDoneCallback(final Consumer<NetworkOperationFuture> doneCallback) {
		this.doneCallback = doneCallback;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.NetworkOperationFuture#getExecutions()
	 */
	@Override
	public int getExecutions() {
		return executions.get();
	}
	
	/**
	 * Get the total number of retries before the future fails
	 */
	public int getTotalRetries() {
		return totalRetries;
	}

	/**
	 * Set the total number of retries before the future fails
	 * @param totalRetries
	 */
	public void setTotalRetries(int totalRetries) {
		this.totalRetries = totalRetries;
	}

	@Override
	public Set<Long> getAffectedRegionIDs() {
		final List<RoutingHop> routingList = lastTransmittedPackage.getRoutingHeader().getRoutingList();
		
		return routingList.stream()
				.flatMap(r -> r.getDistributionRegions().keySet().stream())
				.collect(Collectors.toSet());
	}

}

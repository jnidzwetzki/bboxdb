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
package org.bboxdb.network.client.future.client;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;

public class OperationFutureImpl<T> implements OperationFuture, FutureErrorCallback {

    /**
     * The tuple send delayer
     */
    private final static ScheduledExecutorService scheduler;

    static {
    	scheduler = Executors.newScheduledThreadPool(1);
    }

	/**
	 * The futures
	 */
	protected List<NetworkOperationFuture> futures;

	/**
	 * The default retry policy
	 */
	private FutureRetryPolicy retryPolicy;

	/**
	 * The ready latch
	 */
	protected final CountDownLatch readyLatch = new CountDownLatch(1);

	/**
	 * The retry counter
	 * @param future
	 */
	private int globalRetryCounter = 0;

	/**
	 * The future supplier
	 */
	private Supplier<List<NetworkOperationFuture>> futureSupplier;

	public OperationFutureImpl(final Supplier<List<NetworkOperationFuture>> futures) {
		this(futures, FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES);
	}

	public OperationFutureImpl(final Supplier<List<NetworkOperationFuture>> futureSupplier,
			final FutureRetryPolicy retryPolicy) {

		this.futureSupplier = futureSupplier;
		this.retryPolicy = retryPolicy;

		execute();
	}

	/**
	 * Execute the network operation futures
	 */
	public void execute() {

		// Get futures from supplier
		futures = futureSupplier.get();

		// Set callbacks
		futures.forEach(f -> f.setErrorCallback(this));
		futures.forEach(f -> f.setSuccessCallback((c) -> handleNetworkFutureSuccess()));

		// Execute
		futures.forEach(f -> f.execute());

		// Maybe we have a empty future list, so no callback is executed. Let's
		// check if we are already done
		handleNetworkFutureSuccess();
	}

	/**
	 * Handle a future success
	 * @param future
	 */
	private void handleNetworkFutureSuccess() {
		final boolean allDone = futures.stream().allMatch(f -> f.isDone());

		// If some futures are failed, cancel the successful ones
		if(isFailed()) {
			cancelAllFutures();
		}

		if(allDone) {
			readyLatch.countDown();
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getRequestId(int)
	 */
	@Override
	public short getRequestId(final int resultId) {
		checkFutureSize(resultId);

		return futures.get(resultId).getRequestId();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getMessage(int)
	 */
	@Override
	public String getMessage(final int resultId) {
		checkFutureSize(resultId);

		return futures.get(resultId).getMessage();
	}

	@Override
	public BBoxDBConnection getConnection(final int resultId) {
		checkFutureSize(resultId);

		return futures.get(resultId).getConnection();
	}

	@Override
	public boolean isCompleteResult(int resultId) {
		checkFutureSize(resultId);

		return futures.get(resultId).isCompleteResult();
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getAllMessages()
	 */
	@Override
	public String getAllMessages() {
		return futures
		.stream()
		.filter(f -> f.getMessage() != null)
		.map(f -> f.getMessageWithConnectionName())
		.collect(Collectors.joining(",", "[", "]"));
	}

	/**
	 * Throw an exception when the result id is unknown
	 * @param resultId
	 */
	protected void checkFutureSize(final int resultId) {
		if(resultId > futures.size()) {
			throw new IllegalArgumentException("Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#isFailed()
	 */
	@Override
	public boolean isFailed() {
		return futures.stream().anyMatch(f -> f.isFailed());
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#isDone()
	 */
	@Override
	public boolean isDone() {
		return readyLatch.getCount() == 0;
	}

	/* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#getNumberOfResultObjets()
	 */
	@Override
	public int getNumberOfResultObjets() {
		return futures.size();
	}

	/**
	 * Get the result of the future
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public T get(final int resultId) throws InterruptedException {
		checkFutureSize(resultId);

		return (T) futures.get(resultId).get(true);
	}

    /* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#waitForAll()
	 */
	@Override
	public void waitForCompletion() throws InterruptedException {
		readyLatch.await();
	}

    /* (non-Javadoc)
	 * @see org.bboxdb.network.client.future.OperationFuture#waitForAll()
	 */
	@Override
	public void waitForCompletion(final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException {

		readyLatch.await(timeout, unit);
	}

	@Override
	public long getCompletionTime(final TimeUnit timeUnit) {
		return futures.stream().mapToLong(f -> f.getCompletionTime(timeUnit)).sum();
	}

	/**
	 * Set the retry policy
	 * @param retry
	 */
	public void setRetryPolicy(final FutureRetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * Cancel all old queries
	 */
	public void cancelAllFutures() {
		futures.forEach(f -> cancelOldFuture(f));
	}

	/**
	 * Cancel the old future
	 * @param future
	 */
	private void cancelOldFuture(final NetworkOperationFuture future) {
		final NetworkRequestPackage transmittedPackage = future.getTransmittedPackage();

		if(transmittedPackage == null) {
			return;
		}

		if(! transmittedPackage.needsToBeCanceled()) {
			return;
		}

		// Only successful futures needs to be canceled
		if(future.isFailed()) {
			return;
		}

		final BBoxDBConnection connection = future.getConnection();
		final BBoxDBClient bboxDBClient = connection.getBboxDBClient();
		bboxDBClient.cancelRequest(transmittedPackage.getSequenceNumber());
	}

	/**
	 * Handle the error callback
	 */
	@Override
	public boolean handleError(final NetworkOperationFuture future) {

		if(retryPolicy == FutureRetryPolicy.RETRY_POLICY_NONE) {
			return false;
		}

		if(retryPolicy == FutureRetryPolicy.RETRY_POLICY_ONE_FUTURE) {
			assert (futures.contains(future)) : "Future is unknown: " + future + "/" 
					+ futures.stream().map(f -> f.getRequestId()).collect(Collectors.toList());

			return handleOneFutureRetry(future);
		}

		if(retryPolicy == FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES) {
			return handleAllFutureRetry();
		}

		throw new RuntimeException("Unknown retry policy: " + retryPolicy);
	}

	/**
	 * Handle all futures retry
	 *
	 * @param future
	 * @return
	 */
	private boolean handleOneFutureRetry(final NetworkOperationFuture future) {
		if(future.getExecutions() > future.getTotalRetries()) {
			return false;
		}

		final Runnable futureTask = () -> {
			cancelOldFuture(future);
			future.execute();
		};

		final int delay = (int) (100 * future.getExecutions());
		scheduler.schedule(futureTask, delay, TimeUnit.MILLISECONDS);

		return true;
	}

	/**
	 * Handle one future retry
	 *
	 * @return
	 */
	private boolean handleAllFutureRetry() {
		if(globalRetryCounter >= futures.get(0).getTotalRetries()) {
			return false;
		}

		if(futureSupplier == null) {
			throw new RuntimeException("Error policy is RETRY_ALL_FUTURES and supplier is null");
		}

		globalRetryCounter++;

		final Runnable futureTask = () -> {
			cancelAllFutures();
			execute();
		};

		final int delay = (int) (100 * globalRetryCounter);
		scheduler.schedule(futureTask, delay, TimeUnit.MILLISECONDS);

		return true;
	}

	/**
	 * Get the number of needed executions
	 * @return
	 */
	@Override
	public int getNeededExecutions() {
		return globalRetryCounter + 1;
	}
}

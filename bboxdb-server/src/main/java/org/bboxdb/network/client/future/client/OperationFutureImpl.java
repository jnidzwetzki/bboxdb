/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.network.NetworkOperationFuture;
import org.bboxdb.network.packets.NetworkRequestPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	 * 
	 * @param future
	 */
	private int globalRetryCounter = 0;

	/**
	 * The future supplier
	 */
	private Supplier<List<NetworkOperationFuture>> futureSupplier;
	
	/**
	 * The success callbacks
	 */
	protected final List<Consumer<OperationFuture>> successCallbacks;
	
	/**
	 * Are the success callbacks already run?
	 */
	protected boolean successCallbacksRun = false;

	/**
	 * The failure callbacks
	 */
	protected final List<Consumer<OperationFuture>> failureCallbacks;
	
	/**
	 * Are the failure callbacks already run?
	 */
	protected boolean failureCallbacksRun = false;
	
	/**
	 * The shutdown callbacks
	 */
	protected final List<Consumer<OperationFuture>> shutdownCallbacks = new ArrayList<>();
	
	/**
	 * The error history of the future
	 */
	private final StringBuilder errorHistory = new StringBuilder();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(OperationFutureImpl.class);

	public OperationFutureImpl(final Supplier<List<NetworkOperationFuture>> futures) {
		this(futures, FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES);
	}

	public OperationFutureImpl(final Supplier<List<NetworkOperationFuture>> futureSupplier,
			final FutureRetryPolicy retryPolicy) {

		this(futureSupplier, retryPolicy, new ArrayList<>(), new ArrayList<>());
	}
	
	public OperationFutureImpl(final Supplier<List<NetworkOperationFuture>> futureSupplier,
			final FutureRetryPolicy retryPolicy, List<Consumer<OperationFuture>> successCallbacks,
			List<Consumer<OperationFuture>> failureCallbacks) {

		this.futureSupplier = futureSupplier;
		this.retryPolicy = retryPolicy;
		this.successCallbacks = successCallbacks;
		this.failureCallbacks = failureCallbacks;

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
		futures.forEach(f -> f.setDoneCallback((c) -> handleNetworkFutureDone(c)));

		// Execute
		futures.forEach(f -> f.execute());

		// Maybe we have a empty future list, so no callback is executed. Let's
		// check if we are already done
		handleNetworkFutureDone(null);
	}

	/**
	 * Handle a future done
	 * @param c 
	 * 
	 * @param future
	 */
	private void handleNetworkFutureDone(final NetworkOperationFuture networkOperationFuture) {
		final boolean allDone = futures.stream().allMatch(f -> f.isDone());

		if(logger.isDebugEnabled()) {
			if(networkOperationFuture != null) {
				logger.debug("Got callback from {}", networkOperationFuture.getConnection().getConnectionName());
			}
			
			final long doneFutures = futures.stream().filter(f -> f.isDone()).count();
	
			logger.debug("Handle success, all futures done {} (done={} of={})", allDone, 
					doneFutures, futures.size());
		}
		
		// If some futures are failed, cancel the successful ones
		if (isFailed()) {
			cancelAllFutures();
		}

		if (allDone) {
			
			if(networkOperationFuture != null && ! networkOperationFuture.isFailed()) {
				runSuccessCallbacks();
			}
			
			readyLatch.countDown();
		}
	}

	/**
	 * Run the complete callbacks
	 */
	public void runSuccessCallbacks() {
		successCallbacksRun = true;
		successCallbacks.forEach(c -> c.accept(this));
	}
	
	/**
	 * Run the failure callbacks
	 */
	public void runFailureCallbacks() {
		failureCallbacksRun = true;
		failureCallbacks.forEach(c -> c.accept(this));
	}
	
	/**
	 * Run the complete callbacks
	 */
	public void runShutdownCallbacks() {
		shutdownCallbacks.forEach(c -> c.accept(this));
	}
	
	/**
	 * Add the consumer to the list of success callbacks
	 * @param consumer
	 */
	public void addSuccessCallbackConsumer(final Consumer<OperationFuture> consumer) {
		successCallbacks.add(consumer);
		
		// Run late callback immediately
		if(successCallbacksRun) {
			consumer.accept(this);
		}
	}
	
	/**
	 * Add the consumer to the list of failure callbacks
	 * @param consumer
	 */
	public void addFailureCallbackConsumer(final Consumer<OperationFuture> consumer) {
		failureCallbacks.add(consumer);
		
		// Run late callback immediately
		if(failureCallbacksRun) {
			consumer.accept(this);
		}
	}
	
	/**
	 * Add the future to the lust of shutdown futures
	 * @param consumer
	 */
	public void addShutdownCallbackConsumer(final Consumer<OperationFuture> consumer) {
		shutdownCallbacks.add(consumer);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bboxdb.network.client.future.OperationFuture#getRequestId(int)
	 */
	@Override
	public short getRequestId(final int resultId) {
		checkFutureSize(resultId);

		return futures.get(resultId).getRequestId();
	}

	/*
	 * (non-Javadoc)
	 * 
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bboxdb.network.client.future.OperationFuture#getAllMessages()
	 */
	@Override
	public String getAllMessages() {
		return futures.stream().filter(f -> f.getMessage() != null).map(f -> f.getMessageWithConnectionName())
				.collect(Collectors.joining(",", "[", "]"));
	}

	/**
	 * Throw an exception when the result id is unknown
	 * 
	 * @param resultId
	 */
	protected void checkFutureSize(final int resultId) {
		if (resultId > futures.size()) {
			throw new IllegalArgumentException(
					"Unable to access future with id: " + resultId + "(total " + futures.size() + ")");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bboxdb.network.client.future.OperationFuture#isFailed()
	 */
	@Override
	public boolean isFailed() {
		return futures.stream().anyMatch(f -> f.isFailed());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bboxdb.network.client.future.OperationFuture#isDone()
	 */
	@Override
	public boolean isDone() {
		return readyLatch.getCount() == 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.bboxdb.network.client.future.OperationFuture#getNumberOfResultObjets()
	 */
	@Override
	public int getNumberOfResultObjects() {
		return futures.size();
	}

	/**
	 * Get the result of the future
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public T get(final int resultId) throws InterruptedException {
		checkFutureSize(resultId);

		return (T) futures.get(resultId).get(true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bboxdb.network.client.future.OperationFuture#waitForAll()
	 */
	@Override
	public void waitForCompletion() throws InterruptedException {
		readyLatch.await();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.bboxdb.network.client.future.OperationFuture#waitForAll()
	 */
	@Override
	public void waitForCompletion(final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException {

		readyLatch.await(timeout, unit);
	}

	@Override
	public long getCompletionTime(final TimeUnit timeUnit) {
		return futures.stream()
				.filter(f -> (f.isDone() || f.isFailed()))
				.mapToLong(f -> f.getCompletionTime(timeUnit))
				.sum();
	}

	/**
	 * Set the retry policy
	 * 
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
	 * 
	 * @param future
	 */
	private void cancelOldFuture(final NetworkOperationFuture future) {
		final NetworkRequestPacket transmittedPackage = future.getTransmittedPackage();

		logger.debug("Canceling future [seq={}, connection={}, state={}]", future.getRequestId(),
				future.getConnection().getConnectionName(), future.getConnection().getConnectionState());

		if (transmittedPackage == null) {
			return;
		}

		if (!transmittedPackage.needsToBeCanceled()) {
			return;
		}

		// Only successful futures needs to be canceled
		if (future.isFailed()) {
			return;
		}

		final BBoxDBConnection connection = future.getConnection();
		final BBoxDBClient bboxDBClient = connection.getBboxDBClient();
		bboxDBClient.cancelRequest(transmittedPackage.getSequenceNumber());
	}

	/**
	 * Handle the error callback
	 * 
	 * Method is synchronized to prevent mixed callback handling from multiple
	 * connections
	 */
	@Override
	public synchronized boolean handleError(final NetworkOperationFuture future) {

		if (logger.isDebugEnabled()) {
			logger.debug("Got failed future back [policy={}, seq={}, connection={}, state={}]", retryPolicy,
					future.getRequestId(), future.getConnection().getConnectionName(),
					future.getConnection().getConnectionState());
		}

		if (!futures.contains(future)) {
			logger.debug("Future is unknown, all network futures might be re-executed. Ignoring callback");
			return true;
		}

		if (!future.getConnection().isConnected()) {
			logger.debug("Unable to retry future, because connection failed [connection={}, state={}]",
					future.getConnection().getConnectionName(), future.getConnection().getConnectionState());
			runFailureCallbacks();
			return false;
		}

		if (retryPolicy == FutureRetryPolicy.RETRY_POLICY_NONE) {
			runFailureCallbacks();
			return false;
		}

		if (retryPolicy == FutureRetryPolicy.RETRY_POLICY_ONE_FUTURE) {
			return handleOneFutureRetry(future);
		}

		if (retryPolicy == FutureRetryPolicy.RETRY_POLICY_ALL_FUTURES) {
			return handleAllFutureRetry(future);
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
		
		if (future.getExecutions() > future.getTotalRetries()) {
			runFailureCallbacks();
			return false;
		}

		final Runnable futureTask = () -> {
			
			// Already finished
			if(readyLatch.getCount() == 0) {
				return;
			}
			
			cancelOldFuture(future);
			future.execute();
		};

		final int delay = (int) (100 * future.getExecutions());
		
		errorHistory.append("================================\n");
		errorHistory.append("Schedule in " + delay);
		errorHistory.append(future.getMessageWithConnectionName());
		
		scheduler.schedule(futureTask, delay, TimeUnit.MILLISECONDS);
				
		logger.debug("Reschedule event for type [delay={}, type={}] in handleOneFutureRetry", delay, future.getClass().toString());

		return true;
	}

	/**
	 * Handle one future retry
	 *
	 * @return
	 */
	private boolean handleAllFutureRetry(final NetworkOperationFuture future) {
		
		if (globalRetryCounter >= futures.get(0).getTotalRetries()) {
			runFailureCallbacks();
			return false;
		}

		if (futureSupplier == null) {
			throw new RuntimeException("Error policy is RETRY_ALL_FUTURES and supplier is null");
		}

		globalRetryCounter++;

		final Runnable futureTask = () -> {
			
			// Already finished
			if(readyLatch.getCount() == 0) {
				return;
			}
			
			cancelAllFutures();
			execute();
		};


		final int delay = (int) (100 * globalRetryCounter);
		
		errorHistory.append("================================\n");
		errorHistory.append("Schedule in " + delay);
		errorHistory.append(future.getMessageWithConnectionName());
		
		scheduler.schedule(futureTask, delay, TimeUnit.MILLISECONDS);

		logger.debug("Reschedule event for type [delay={}, type={}] in handleAllFutureRetry", delay, future.getClass().toString());

		return true;
	}

	/**
	 * Get the number of needed executions
	 * 
	 * @return
	 */
	@Override
	public int getNeededExecutions() {
		return globalRetryCounter + 1;
	}

	@Override
	public Set<Long> getAffectedRegionIDs() {
		final Set<Long> regions = new HashSet<>();
		
		for(final NetworkOperationFuture future : futures) {
			regions.addAll(future.getAffectedRegionIDs());
		}
		
		return regions;
	}

	@Override
	public String getErrorLog() {
		return errorHistory.toString();
	}
}

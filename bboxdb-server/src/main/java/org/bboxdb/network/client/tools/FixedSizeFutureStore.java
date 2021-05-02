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
package org.bboxdb.network.client.tools;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bboxdb.network.client.future.client.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

public class FixedSizeFutureStore {

	/**
	 * The amount of max pending futures
	 */
	private final long maxPendingFutures;

	/**
	 * The failed future callback
	 */
	private final List<Consumer<OperationFuture>> failedFutureCallbacks;

	/**
	 * The future counter
	 */
	private final AtomicLong futureCounter;

	/**
	 * The future counter (pendingFuture, counter)
	 */
	private final Map<OperationFuture, Long> pendingFutureMap;

	/**
	 * The statistics writer
	 */
	private Writer statisticsWriter;
	
	/**
	 * The stopwatch
	 */
	private final Stopwatch stopwatch;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FixedSizeFutureStore.class);
	
	/**
	 * The failed futures log callback
	 */
    private final Consumer<OperationFuture> FAILED_FUTURE_LOG_CALLBACK = (f) -> logger.error("Failed future detected: {} / {}", f, f.getAllMessages());

	public FixedSizeFutureStore(final long maxPendingFutures, final boolean logFailedFutures) {
		this.maxPendingFutures = maxPendingFutures;
		this.failedFutureCallbacks = new ArrayList<>();
		this.statisticsWriter = null;
		this.futureCounter = new AtomicLong();
		this.pendingFutureMap = new HashMap<>();
		this.stopwatch = Stopwatch.createStarted();
		
		if(logFailedFutures) {
			addFailedFutureCallback(FAILED_FUTURE_LOG_CALLBACK);
		}
	}

	/**
	 * Enable statistics output
	 * @throws IOException
	 */
	public void writeStatistics(final Writer writer) {
		this.statisticsWriter = writer;
	}

	/**
	 * Put a new future into the store
	 *
	 * This method might block when to much futures are unfinished
	 */
	public boolean put(final OperationFuture futureToAdd) {
		
		if(futureToAdd == null) {
			return false;
		}
		
		final long futureId = futureCounter.getAndIncrement();
		
		synchronized (pendingFutureMap) {
			pendingFutureMap.put(futureToAdd, futureId);
		}
		
		checkAndCleanupRunningFuture();
		
		return true;
	}

	/**
	 * Check and cleanup running futures
	 */
	private void checkAndCleanupRunningFuture() {
		synchronized (pendingFutureMap) {
			if (pendingFutureMap.size() <= maxPendingFutures) {
				return;
			}
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
	@VisibleForTesting
	public void removeCompleteFutures() {

		synchronized (pendingFutureMap) {
			// Get done futures
			final List<OperationFuture> doneFutures = pendingFutureMap.keySet().stream()
				.filter(f -> f.isDone())
				.collect(Collectors.toList());
			
			if(logger.isDebugEnabled()) {
				logger.debug("Removed {} futures", doneFutures.size());
			}
			
			// Handle failed futures
			doneFutures.stream()
					.filter(f -> f.isFailed())
					.forEach(f -> handleFailedFuture(f));
	
			writeStatisticsAndRemoveFuturesFromMap(doneFutures);
		}
	}

	/**
	 * Write performance statistics
	 * @param doneFutures
	 */
	private void writeStatisticsAndRemoveFuturesFromMap(final List<OperationFuture> doneFutures) {

		synchronized (pendingFutureMap) {

			for(final OperationFuture future : doneFutures) {
				final Long futureNumberLong = pendingFutureMap.remove(future);
				
				final long futureNumber = futureNumberLong != null ? futureNumberLong : -1;
				
				final long completionTime = future.getCompletionTime(TimeUnit.MILLISECONDS);
				final int executions = future.getNeededExecutions();
	
				if(statisticsWriter != null) {
					final String outputValue = String.format("%d\t%d\t%d\t%d%n", 
							stopwatch.elapsed(TimeUnit.MICROSECONDS), 
							futureNumber, completionTime, executions);
	
					try {
						statisticsWriter.write(outputValue);
					} catch (IOException e) {
						logger.error("Got IO exception while writing statistics", e);
					}
				}
			}
		}

	}

	/**
	 * Handle a failed future
	 * @param future
	 */
	private void handleFailedFuture(final OperationFuture future) {
		failedFutureCallbacks.forEach(c -> c.accept(future));
	}

	/**
	 * Is the future cleanup needed?
	 * @return
	 */
	private boolean isCleanupNeeded() {
		synchronized (pendingFutureMap) {
			return pendingFutureMap.size() > maxPendingFutures * 0.8;
		}
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
		synchronized (pendingFutureMap) {
			return pendingFutureMap.size();
		}
	}

	/**
	 * Add a new failed future callback
	 */
	public void addFailedFutureCallback(final Consumer<OperationFuture> callback) {
		failedFutureCallbacks.add(callback);
	}

	/**
	 * Wait for the completion of all pending futures
	 * @throws InterruptedException
	 *
	 */
	public void waitForCompletion() throws InterruptedException {

		synchronized (pendingFutureMap) {
			while(! pendingFutureMap.isEmpty()) {
	
				for(final OperationFuture future : pendingFutureMap.keySet()) {
					future.waitForCompletion();
				}
	
				removeCompleteFutures();
			}
		}
	}

}

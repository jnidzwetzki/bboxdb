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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.ServiceState;
import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.commons.concurrent.ThreadHelper;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.entity.Tuple;

public class TupleListFutureStore {

	/**
	 * The number of request worker
	 */
	protected final int requestWorker; 
	
	/**
	 * The maximal amount of unfinished worker
	 */
	protected final int maxQueueSize;
	
	/**
	 * The unfinished future queue
	 */
	protected final BlockingQueue<TupleListFuture> futureQueue;
	
	/**
	 * The list of running threads
	 */
	protected final List<Thread> runningThreads;
	
	/**
	 * The amount of active worker
	 */
	protected final AtomicInteger activeWorker;
	
	/**
	 * The service state
	 */
	protected final ServiceState serviceState;
	
	/**
	 * The default amount of worker
	 */
	public final static int DEFAULT_REQUEST_WORKER = 10;
	
	/**
	 * The default max queue size
	 */
	public final static int DEFAULT_MAX_QUEUE_SIZE = DEFAULT_REQUEST_WORKER * 2;
	
	public TupleListFutureStore() {
		this(DEFAULT_REQUEST_WORKER, DEFAULT_MAX_QUEUE_SIZE);
	}

	public TupleListFutureStore(final int requestWorker, final int maxQueueSize) {
		this.serviceState = new ServiceState();
		this.requestWorker = requestWorker;
		this.maxQueueSize = maxQueueSize;
		this.futureQueue = new LinkedBlockingQueue<>(maxQueueSize);
		this.runningThreads = new ArrayList<>(requestWorker);
		this.activeWorker = new AtomicInteger(0);
		
		serviceState.dipatchToStarting();
		
		for(int i = 0; i < requestWorker; i++) {
			final RequestWorker requestWorkerInstance = new RequestWorker(futureQueue, activeWorker);
			final Thread thread = new Thread(requestWorkerInstance);
			thread.start();
			runningThreads.add(thread);
		}
		
		serviceState.dispatchToRunning();
	}

	/**
	 * Add a new future
	 * @param tupleListFuture
	 * @throws InterruptedException 
	 * @throws RejectedException 
	 */
	public void put(final TupleListFuture tupleListFuture) 
			throws InterruptedException, RejectedException {
		
		if(! serviceState.isInRunningState()) {
			throw new RejectedException("Service is in state: " + serviceState.getState());
		}
		
		synchronized (futureQueue) {
			futureQueue.put(tupleListFuture);
			futureQueue.notifyAll();
		}
	}
	
	/**
	 * Shutdown the store
	 */
	public void shutdown() {
		
		if(! serviceState.isInRunningState()) {
			return;
		}
		
		serviceState.dispatchToStopping();
		
		ThreadHelper.stopThreads(runningThreads);
		runningThreads.clear();
		
		serviceState.dispatchToTerminated();
	}
	
	/**
	 * Wait for the completion
	 * @throws InterruptedException
	 */
	public void waitForCompletion() throws InterruptedException {
		synchronized (activeWorker) {
			while(activeWorker.get() != 0 || ! futureQueue.isEmpty()) {
				activeWorker.wait();
			}
		}
	}
	
	/**
	 * Get the service state
	 * @return
	 */
	public ServiceState getServiceState() {
		return serviceState;
	}
	
	/**
	 * Get the number of request worker
	 * @return
	 */
	public int getRequestWorker() {
		return requestWorker;
	}
	
	/**
	 * Get the max queue size
	 * @return
	 */
	public int getMaxQueueSize() {
		return maxQueueSize;
	}
}

class RequestWorker extends ExceptionSafeThread {
	
	/**
	 * The queue
	 */
	private BlockingQueue<TupleListFuture> queue;
	
	/**
	 * The active worker counter
	 */
	private AtomicInteger activeWorker;

	public RequestWorker(final BlockingQueue<TupleListFuture> queue, final AtomicInteger activeWorker) {
		this.queue = queue;
		this.activeWorker = activeWorker;
	}

	@Override
	protected void runThread() throws Exception {
	
		while(! Thread.currentThread().isInterrupted()) {
			try {
				final TupleListFuture future = queue.take();
				
				synchronized (activeWorker) {
					activeWorker.incrementAndGet();
					activeWorker.notifyAll();
				}

				if(future != null) {
					future.waitForAll();
					final Iterator<Tuple> iter = future.iterator();
					while(iter.hasNext()) {
						iter.next();
					}
				}
				
				synchronized (activeWorker) {
					activeWorker.decrementAndGet();
					activeWorker.notifyAll();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		
	}
	
}

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
package org.bboxdb.commons.concurrent;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingQueueWithSingleExecutor {

	/**
	 * The red pill
	 */
	private final Runnable RED_PILL = () -> {};
	
	/**
	 * The pending callbacks
	 */
	protected final LinkedBlockingQueue<Runnable> pendingRunables;
	
	/**
	 * The executor runnable
	 */
	protected final Runnable executorRunnable;
	
	/**
	 * The executor
	 */
	protected final Thread executor;
	
	/**
	 * Shutdown state
	 */
	protected boolean shutdown;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BlockingQueueWithSingleExecutor.class);
	
	public BlockingQueueWithSingleExecutor(final String name, final int maxQueueSize) {
		pendingRunables = new LinkedBlockingQueue<>(maxQueueSize);
		executorRunnable = new ExceptionSafeRunnable() {
			
			@Override
			protected void runThread() throws Exception {
				while(! Thread.currentThread().isInterrupted()) {
					final Runnable runnable = pendingRunables.take();
					
					if(runnable.equals(RED_PILL)) {
						return;
					}
					
					runnable.run();
				}
			}
			
			@Override
			protected void beginHook() {
				super.beginHook();
				logger.debug("Starting new BlockingQueueWithSingleExecutor");
			}
			
			@Override
			protected void endHook() {
				super.endHook();
				logger.debug("Stopped new BlockingQueueWithSingleExecutor");
			}
		};
		
		shutdown = false;
		executor = new Thread(executorRunnable);
		executor.setName(name);
		executor.start();
	}
	
	/**
	 * Queue the given runnable
	 * @param runnable
	 * @return 
	 * @throws InterruptedException
	 */
	public boolean queue(final Runnable runnable) throws InterruptedException {
		
		if(shutdown) {
			return false;
		}
		
		pendingRunables.put(runnable);
		
		return true;
	}
	
	/**
	 * Shutdown the queue
	 * @throws InterruptedException 
	 */
	public void shutdown(boolean discardPending) throws InterruptedException {
		shutdown = true;
		
		if(discardPending) {
			pendingRunables.clear();
		}
		
		pendingRunables.put(RED_PILL);
	}
	
	/**
	 * Is the executor active
	 * @return
	 */
	public boolean isExecutorActive() {
		return executor.isAlive();
	}
	
}

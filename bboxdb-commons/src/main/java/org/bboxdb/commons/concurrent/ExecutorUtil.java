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
package org.bboxdb.commons.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorUtil {

	/***
	 * Returns an executor that holds up to maxQueueSize unprocessed tasks 
	 * and processes maxThreads in parallel. When more than maxQueueSize tasks 
	 * are submitted, the task is rejected from the queue and executed in the
	 * calling thread directly
	 * 
	 * @param maxThreads
	 * @param maxQueueSize
	 * @return
	 */
	public static ThreadPoolExecutor getBoundThreadPoolExecutor(final int maxThreads, final int maxQueueSize) {
		// The queue holds up to maxQueueSize unexecuted tasks
		final BlockingQueue<Runnable> linkedBlockingDeque = new LinkedBlockingDeque<Runnable>(maxQueueSize);
		
		// The Pool executor executes up to maxThreads in parallel. If the queue is full, the caller
		// has to execute the task directly. 
		return new ThreadPoolExecutor(maxThreads / 2, maxThreads, 30, TimeUnit.SECONDS, 
				linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy());
	}

}

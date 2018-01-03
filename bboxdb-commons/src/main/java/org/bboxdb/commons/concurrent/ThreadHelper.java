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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadHelper {

	/**
	 * The timeout for a thread join (10 seconds)
	 */
	public static long THREAD_WAIT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ThreadHelper.class);
	
	/**
	 * Stop the running threads
	 * @param runningThreads
	 * @return
	 */
	public static boolean stopThreads(final List<? extends Thread> runningThreads) {
		
		boolean result = true;
		
		// Interrupt the running threads
		logger.info("Interrupt running service threads");
		runningThreads.forEach(t -> t.interrupt());
		
		// Join threads
		for(final Thread thread : runningThreads) {
			try {
				logger.info("Join thread: {}", thread.getName());

				thread.join(THREAD_WAIT_TIMEOUT);
				
				// Is the thread still alive?
				if(thread.isAlive()) {
					logger.error("Unable to stop thread: {}", thread.getName());
					result = false;
				}
				
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("Got exception while waiting on thread join: " + thread.getName(), e);
				result = false;
			}
		}
		
		return result;
	}
}

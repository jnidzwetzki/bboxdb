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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExceptionSafeRunnable implements Runnable {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ExceptionSafeRunnable.class);

	/**
	 * Run method that catches all throwables
	 */
	@Override
	public void run() {
		try {
			// Begin hook
			beginHook();
			
			// Do the real work
			runThread();
			
			// End hook
			endHook();
		} catch(Throwable e) {
			logger.error("Got exception during thread execution", e);
			afterExceptionHook();
		}
	}

	/**
	 * The after exception hook. Will be called after an exception.
	 */
	protected void afterExceptionHook() {
		// Default: Do nothing
	}
	
	/**
	 * The begin hook
	 */
	protected void beginHook() {
		// Default: Do nothing
	}
	
	/**
	 * The end hook
	 */
	protected void endHook() {
		// Default: Do nothing
	}
	
	/**
	 * The real run method
	 * @throws Exception 
	 */
	protected abstract void runThread() throws Exception;

}

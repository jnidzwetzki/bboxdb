/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.util;

import org.bboxdb.network.routing.PackageRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExceptionSafeThread implements Runnable {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PackageRouter.class);

	/**
	 * Run method that catches throwables
	 */
	@Override
	public void run() {
		try {
			runThread();
		} catch(Throwable e) {
			logger.error("Got exception during thread execution", e);
		}
	}

	/**
	 * The real run method
	 */
	protected abstract void runThread();

}

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
package org.bboxdb.jmx;

import org.bboxdb.BBoxDBMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lifecycle implements LifecycleMBean {
	
	/**
	 * The instance of the application
	 */
	protected final BBoxDBMain bBoxDBMain;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Lifecycle.class);


	public Lifecycle(final BBoxDBMain bboxDBMain) {
		this.bBoxDBMain = bboxDBMain;
	}

	@Override
	public String getName() {
		return "BBoxDB lifecycle MBean";
	}

	@Override
	public void shutdown() {
		
		logger.info("Got shutdown call via MBean");
		
		try {
			bBoxDBMain.stop();
		} catch (Exception e) {
			logger.warn("Got an exception while stopping application");
		}
		
		System.exit(0);
	}

}

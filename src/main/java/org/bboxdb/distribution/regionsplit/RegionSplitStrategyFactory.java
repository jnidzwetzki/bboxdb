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
package org.bboxdb.distribution.regionsplit;

import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitStrategyFactory {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionSplitStrategyFactory.class);

	/**
	 * Get an instance of the configured factory
	 * @return
	 */
	public static AbstractRegionSplitStrategy getInstance() {
		
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		final String factoryClass = configuration.getRegionSplitStrategy();
		
		if("none".equals(factoryClass)) {
			return null;
		}
		
		// Instance the classname
		try {
			final Class<?> classObject = Class.forName(factoryClass);
			
			if(classObject == null) {
				throw new ClassNotFoundException("Unable to locate class: " + factoryClass);
			}
			
			final Object factoryObject = classObject.newInstance();
			
			if(! (factoryObject instanceof AbstractRegionSplitStrategy)) {
				throw new ClassNotFoundException(factoryClass + " is not a instance of RegionSplitter");
			}
			
			return (AbstractRegionSplitStrategy) factoryObject;
						
		} catch (Exception e) {
			logger.warn("Unable to instance class: " + factoryClass, e);
			throw new RuntimeException(e);
		} 
	}
}

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
package org.bboxdb.distribution.allocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceAllocatorFactory {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ResourceAllocatorFactory.class);

	/**
	 * Get an instance of the configured factory
	 * @return
	 */
	public static ResourceAllocator getInstance(final String placementStrategy) {
		
		if("none".equals(placementStrategy)) {
			return null;
		}
		
		// Instance the classname
		try {
			final Class<?> classObject = Class.forName(placementStrategy);
			
			if(classObject == null) {
				throw new ClassNotFoundException("Unable to locate class: " + placementStrategy);
			}
			
			final Object factoryObject = classObject.newInstance();
			
			if(! (factoryObject instanceof ResourceAllocator)) {
				throw new ClassNotFoundException(placementStrategy + " is not a instance of ReplicationStrategy");
			}
			
			return (ResourceAllocator) factoryObject;			
		} catch (Exception e) {
			logger.warn("Unable to instance class: " + placementStrategy, e);
			throw new RuntimeException(e);
		} 
	}
	
}

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
package org.bboxdb.distribution.region;

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionSyncerHelper {
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionSyncerHelper.class);

	/**
	 * Wait for predicate
	 * @param predicate
	 * @param region
	 */
	public static void waitForPredicate(final Predicate<DistributionRegion> predicate, 
			final DistributionRegion region, final DistributionRegionSyncer syncer) {
		
		final Object MUTEX = new Object();
		
		final DistributionRegionCallback callback = (e, r) -> {
			synchronized (MUTEX) {
				MUTEX.notifyAll();
			}
		};
		
		syncer.registerCallback(callback);
		
		// Wait for zookeeper callback
		synchronized (MUTEX) {
			while(! predicate.test(region)) {
				logger.debug("Wait for zookeeper callback for predicate for: {}", region);
				try {
					MUTEX.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.warn("Unable to wait for predicate for: {}", region);
				}
			}
		}
		
		syncer.unregisterCallback(callback);
	}
}

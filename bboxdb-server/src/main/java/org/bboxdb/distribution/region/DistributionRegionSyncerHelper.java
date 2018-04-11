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

import java.util.concurrent.CountDownLatch;
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
	 * @throws InterruptedException 
	 */
	public static void waitForPredicate(final Predicate<DistributionRegion> predicate, 
			final DistributionRegion region, final DistributionRegionSyncer syncer) 
					throws InterruptedException {
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		final DistributionRegionCallback callback = (e, r) -> {
			if(predicate.test(region)) {
				logger.debug("Predicate {} is true, wake up latch", predicate);
				latch.countDown();
			}
		};
		
		syncer.registerCallback(callback);
		
		// Test if predicate is already true (condition was true, when we were called)
		// In this case, we don't need to wait
		if(! predicate.test(region)) {
			logger.debug("Wait for predicate {}", predicate);
			latch.await();
		}
		
		syncer.unregisterCallback(callback);
	}
}

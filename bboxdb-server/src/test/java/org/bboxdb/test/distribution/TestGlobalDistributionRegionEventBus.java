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
package org.bboxdb.test.distribution;

import java.util.concurrent.atomic.AtomicLong;

import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
import org.bboxdb.distribution.region.GlobalDistributionRegionEventBus;
import org.junit.Assert;
import org.junit.Test;

public class TestGlobalDistributionRegionEventBus {

	/**
	 * Test the event handling
	 */
	@Test(timeout = 60_000)
	public void testEvents() {
		
		final AtomicLong calls = new AtomicLong(0);
		
		final DistributionRegionCallback cb = (r, t) -> { calls.incrementAndGet(); };
		
		GlobalDistributionRegionEventBus.getInstance().registerCallback(cb);
		
		Assert.assertEquals(0,  calls.get());
		
		GlobalDistributionRegionEventBus.getInstance().runCallback(null, DistributionRegionEvent.LOCAL_MAPPING_ADDED);
		Assert.assertEquals(1,  calls.get());
		
		Assert.assertTrue(GlobalDistributionRegionEventBus.getInstance().removeCallback(cb));

		GlobalDistributionRegionEventBus.getInstance().runCallback(null, DistributionRegionEvent.LOCAL_MAPPING_ADDED);
		Assert.assertEquals(1,  calls.get());
		
		Assert.assertFalse(GlobalDistributionRegionEventBus.getInstance().removeCallback(cb));
	}
	
}

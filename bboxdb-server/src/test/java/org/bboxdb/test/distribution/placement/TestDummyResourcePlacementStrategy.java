/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
package org.bboxdb.test.distribution.placement;

import org.bboxdb.distribution.placement.DummyResourcePlacementStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.placement.ResourcePlacementStrategy;
import org.junit.Assert;
import org.junit.Test;

public class TestDummyResourcePlacementStrategy {

	/**
	 * Test the placement
	 * @throws ResourceAllocationException 
	 */
	@Test(timeout=60000)
	public void testPlacement() throws ResourceAllocationException {
		final ResourcePlacementStrategy strategy = new DummyResourcePlacementStrategy();
		
		Assert.assertEquals(DummyResourcePlacementStrategy.DUMMY_INSTANCE, 
				strategy.getInstancesForNewRessource(null));
	}
}

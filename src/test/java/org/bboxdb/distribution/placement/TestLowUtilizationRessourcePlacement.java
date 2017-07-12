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
package org.bboxdb.distribution.placement;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.event.DistributedInstanceState;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class TestLowUtilizationRessourcePlacement extends TestRandomRessourcePlacement {
	
	/**
	 * System Utilization
	 */
	final Multiset<DistributedInstance> utilization = HashMultiset.create();
	
	/**
	 * Get the placement strategy for the test
	 * @return
	 */
	@Override
	public ResourcePlacementStrategy getPlacementStrategy() {
		
		return new LowUtilizationResourcePlacementStrategy() {
			@Override
			protected Multiset<DistributedInstance> calculateSystemUsage() {
				return utilization;
			}
		};
	}

	/**
	 * Test round robin placement 1
	 * @throws ResourceAllocationException
	 */
	@Test
	public void testUtilPlacement() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<DistributedInstance> systems = new ArrayList<>();
		
		systems.add(new DistributedInstance("node1:123", "0.1", DistributedInstanceState.READY));
		systems.add(new DistributedInstance("node2:123", "0.1", DistributedInstanceState.READY));
		systems.add(new DistributedInstance("node3:123", "0.1", DistributedInstanceState.READY));
		systems.add(new DistributedInstance("node4:123", "0.1", DistributedInstanceState.READY));
		
		utilization.clear();
		utilization.setCount(systems.get(0), 1);
		utilization.setCount(systems.get(1), 1);
		utilization.setCount(systems.get(2), 1);
		
		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		utilization.clear();
		utilization.setCount(systems.get(0), 1);
		utilization.setCount(systems.get(1), 2);
		utilization.setCount(systems.get(2), 3);
		utilization.setCount(systems.get(3), 4);

		Assert.assertEquals(systems.get(0), resourcePlacementStrategy.getInstancesForNewRessource(systems));
	}

}

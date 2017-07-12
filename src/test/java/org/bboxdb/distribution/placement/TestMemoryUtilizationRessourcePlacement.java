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

public class TestMemoryUtilizationRessourcePlacement extends TestRandomRessourcePlacement {
	
	/**
	 * System Utilization
	 */
	final Multiset<DistributedInstance> utilization = HashMultiset.create();
	
	/**
	 * Get the placement strategy for the test
	 * @return
	 */
	@Override
	public AbstractUtilizationPlacementStrategy getPlacementStrategy() {
		
		return new MemoryUtilizationPlacementStrategy() {
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
		
		final DistributedInstance instance1 = new DistributedInstance("node1:123", "0.1", DistributedInstanceState.READY);
		systems.add(instance1);
		instance1.setMemory(1000);
		final DistributedInstance instance2 = new DistributedInstance("node2:123", "0.1", DistributedInstanceState.READY);
		systems.add(instance2);
		instance2.setMemory(4000);
		final DistributedInstance instance3 = new DistributedInstance("node3:123", "0.1", DistributedInstanceState.READY);
		systems.add(instance3);
		instance3.setMemory(16000);
		final DistributedInstance instance4 = new DistributedInstance("node4:123", "0.1", DistributedInstanceState.READY);
		systems.add(instance4);
		instance4.setMemory(64000);
		
		utilization.clear();
		utilization.setCount(systems.get(0), 1);
		utilization.setCount(systems.get(1), 1);
		utilization.setCount(systems.get(2), 1);
		
		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		utilization.clear();
		utilization.setCount(systems.get(0), 1); // 1 / 1 = 1
		utilization.setCount(systems.get(1), 2); // 4 / 2 = 2
		utilization.setCount(systems.get(2), 3); // 16 / 3 = 5,3
		utilization.setCount(systems.get(3), 4); // 64 / 4 = 16

		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		// Upgrade memory 64 / 2 = 32
		instance2.setMemory(64000);
		Assert.assertEquals(systems.get(1), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		// Assign a lot of tables
		utilization.setCount(systems.get(1), 64); // 64 / 64 = 1
		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		// Remove memory information about instance4
		instance4.setMemory(-1);
		Assert.assertEquals(systems.get(2), resourcePlacementStrategy.getInstancesForNewRessource(systems));
	}

}

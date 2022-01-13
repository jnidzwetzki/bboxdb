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
package org.bboxdb.test.distribution.placement;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.allocator.AbstractUtilizationAllocator;
import org.bboxdb.distribution.allocator.CPUCoreUtilizationAllocator;
import org.bboxdb.distribution.allocator.ResourceAllocationException;
import org.bboxdb.distribution.allocator.ResourceAllocator;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class TestCPUCoreUtilizationRessourcePlacement extends TestRandomRessourcePlacement {
	
	/**
	 * System Utilization
	 */
	final Multiset<BBoxDBInstance> utilization = HashMultiset.create();
	
	/**
	 * Get the placement strategy for the test
	 * @return
	 */
	@Override
	public AbstractUtilizationAllocator getPlacementStrategy() {
		
		return new CPUCoreUtilizationAllocator() {
			@Override
			protected Multiset<BBoxDBInstance> calculateSystemUsage() {
				return utilization;
			}
		};
	}

	/**
	 * Test round robin placement 1
	 * @throws ResourceAllocationException
	 */
	@Test(timeout=60000)
	public void testUtilPlacement() throws ResourceAllocationException {
		final ResourceAllocator resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<>();
		
		final BBoxDBInstance instance1 = new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance1);
		instance1.setCpuCores(1);
		final BBoxDBInstance instance2 = new BBoxDBInstance("node2:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance2);
		instance2.setCpuCores(4);
		final BBoxDBInstance instance3 = new BBoxDBInstance("node3:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance3);
		instance3.setCpuCores(16);
		final BBoxDBInstance instance4 = new BBoxDBInstance("node4:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance4);
		instance4.setCpuCores(64);
		
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
		
		// Upgrade cpu cores: 64 / 2 = 32
		instance2.setCpuCores(64);
		Assert.assertEquals(systems.get(1), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		// Assign a lot of tables
		utilization.setCount(systems.get(1), 64); // 64 / 64 = 1
		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		// Remove cpu information about instance4
		instance4.setCpuCores(-1);
		Assert.assertEquals(systems.get(2), resourcePlacementStrategy.getInstancesForNewRessource(systems));
	}

}

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
package org.bboxdb.distribution.placement;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class TestStorageUtilizationRessourcePlacement extends TestRandomRessourcePlacement {
	
	/**
	 * System Utilization
	 */
	final Multiset<BBoxDBInstance> utilization = HashMultiset.create();
	
	/**
	 * Get the placement strategy for the test
	 * @return
	 */
	@Override
	public AbstractUtilizationPlacementStrategy getPlacementStrategy() {
		
		return new StorageUtilizationPlacementStrategy() {
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
	@Test
	public void testUtilPlacement() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<>();
		
		final BBoxDBInstance instance1 = new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance1);
		instance1.addFreeSpace("/tmp", 10);
		instance1.addTotalSpace("/tmp", 10);
		
		final BBoxDBInstance instance2 = new BBoxDBInstance("node2:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance2);
		instance2.addFreeSpace("/tmp", 10);
		instance2.addTotalSpace("/tmp", 10);
		instance2.addFreeSpace("/tmp2", 10);
		instance2.addTotalSpace("/tmp2", 10);
		
		final BBoxDBInstance instance3 = new BBoxDBInstance("node3:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance3);
		instance3.addFreeSpace("/tmp", 10);
		instance3.addTotalSpace("/tmp", 10);
		instance3.addFreeSpace("/tmp2", 10);
		instance3.addTotalSpace("/tmp2", 10);
		instance3.addFreeSpace("/tmp3", 10);
		instance3.addTotalSpace("/tmp3", 10);
		
		final BBoxDBInstance instance4 = new BBoxDBInstance("node4:123", "0.1", BBoxDBInstanceState.READY);
		systems.add(instance4);
		instance4.addFreeSpace("/tmp", 10);
		instance4.addTotalSpace("/tmp", 10);
		instance4.addFreeSpace("/tmp2", 10);
		instance4.addTotalSpace("/tmp2", 10);
		instance4.addFreeSpace("/tmp3", 10);
		instance4.addTotalSpace("/tmp3", 10);
		instance4.addFreeSpace("/tmp4", 10);
		instance4.addTotalSpace("/tmp4", 10);
		
		utilization.clear();
		utilization.setCount(systems.get(0), 1);
		utilization.setCount(systems.get(1), 1);
		utilization.setCount(systems.get(2), 1);
		
		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		utilization.clear();
		utilization.setCount(systems.get(0), 1); 
		utilization.setCount(systems.get(1), 1); 
		utilization.setCount(systems.get(2), 1); 
		utilization.setCount(systems.get(3), 1); 

		Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		
		utilization.setCount(systems.get(3), 100); 
		Assert.assertEquals(systems.get(2), resourcePlacementStrategy.getInstancesForNewRessource(systems));
	}

}

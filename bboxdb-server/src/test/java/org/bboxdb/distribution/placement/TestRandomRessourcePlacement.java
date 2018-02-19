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

public class TestRandomRessourcePlacement {
	
	/**
	 * Get the placement strategy for the test
	 * @return
	 */
	public ResourcePlacementStrategy getPlacementStrategy() {
		return new RandomResourcePlacementStrategy();
	}

	/**
	 * Test placement 0 (empty list)
	 * @throws ResourceAllocationException
	 */
	@Test(expected=ResourceAllocationException.class)
	public void testPlacement0() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<BBoxDBInstance>();
		resourcePlacementStrategy.getInstancesForNewRessource(systems);
	}
	
	/**
	 * Test placement 1 (all systems are blacklisted)
	 * @throws ResourceAllocationException
	 */
	@Test(expected=ResourceAllocationException.class)
	public void testPlacement1() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<BBoxDBInstance>();
		systems.add(new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.READY));
		systems.add(new BBoxDBInstance("node2:123", "0.1", BBoxDBInstanceState.READY));
		systems.add(new BBoxDBInstance("node3:123", "0.1", BBoxDBInstanceState.READY));
		systems.add(new BBoxDBInstance("node4:123", "0.1", BBoxDBInstanceState.READY));
		
		Assert.assertEquals(systems.get(0), resourcePlacementStrategy.getInstancesForNewRessource(systems, systems));
	}

	/**
	 * Test placement 2 (only one system)
	 * @throws ResourceAllocationException
	 */
	@Test
	public void testPlacement2() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<BBoxDBInstance>();
		systems.add(new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.READY));

		Assert.assertEquals(systems.get(0), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		Assert.assertEquals(systems.get(0), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		Assert.assertEquals(systems.get(0), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		Assert.assertEquals(systems.get(0), resourcePlacementStrategy.getInstancesForNewRessource(systems));
	}

	/**
	 * No system is ready
	 * @throws ResourceAllocationException 
	 */
	@Test(expected=ResourceAllocationException.class)
	public void testNonReadySystems1() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<BBoxDBInstance>();
		systems.add(new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.OUTDATED));
		systems.add(new BBoxDBInstance("node2:123", "0.1", BBoxDBInstanceState.FAILED));
		systems.add(new BBoxDBInstance("node3:123", "0.1", BBoxDBInstanceState.FAILED));
		systems.add(new BBoxDBInstance("node4:123", "0.1", BBoxDBInstanceState.FAILED));
		
		resourcePlacementStrategy.getInstancesForNewRessource(systems);
	}
	
	/**
	 * No system is ready
	 * @throws ResourceAllocationException 
	 */
	@Test(expected=ResourceAllocationException.class)
	public void testNonReadySystems2() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<BBoxDBInstance>();
		systems.add(new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.OUTDATED));
		systems.add(new BBoxDBInstance("node2:123", "0.1", BBoxDBInstanceState.FAILED));
		systems.add(new BBoxDBInstance("node3:123", "0.1", BBoxDBInstanceState.FAILED));
		systems.add(new BBoxDBInstance("node4:123", "0.1", BBoxDBInstanceState.FAILED));
		
		resourcePlacementStrategy.getInstancesForNewRessource(systems);
	}
	
	/**
	 * Only ready systems should be returned
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testNonReadySystems3() throws ResourceAllocationException {
		final ResourcePlacementStrategy resourcePlacementStrategy = getPlacementStrategy();
		final List<BBoxDBInstance> systems = new ArrayList<BBoxDBInstance>();
		systems.add(new BBoxDBInstance("node1:123", "0.1", BBoxDBInstanceState.OUTDATED));
		systems.add(new BBoxDBInstance("node2:123", "0.1", BBoxDBInstanceState.FAILED));
		systems.add(new BBoxDBInstance("node3:123", "0.1", BBoxDBInstanceState.FAILED));
		systems.add(new BBoxDBInstance("node4:123", "0.1", BBoxDBInstanceState.READY));
		
		for(int i = 0; i < 100; i++) {
			Assert.assertEquals(systems.get(3), resourcePlacementStrategy.getInstancesForNewRessource(systems));
		}
	}
}

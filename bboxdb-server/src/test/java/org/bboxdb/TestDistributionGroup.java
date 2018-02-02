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
package org.bboxdb;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestDistributionGroup {

	protected static ZookeeperClient zookeeperClient;

	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=RuntimeException.class)
	public void createInvalidDistributionGroup1() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = DistributionRegion.createRootElement(new DistributionGroupName("foo__"));
	}
	
	/**
	 * Create an illegal distribution group
	 */
	@Test(expected=RuntimeException.class)
	public void createInvalidDistributionGroup2() {
		@SuppressWarnings("unused")
		final DistributionRegion distributionRegion = DistributionRegion.createRootElement(new DistributionGroupName("12_foo_bar"));
	}
	
	/**
	 * Test a distribution region with only one level
	 */
	@Test
	public void testLeafNode() {
		final DistributionRegion distributionRegion = createDistributionGroup(2);
		Assert.assertTrue(distributionRegion.isLeafRegion());
		Assert.assertEquals(2, distributionRegion.getDimension());
		Assert.assertEquals(0, distributionRegion.getLevel());
		
		Assert.assertEquals(1, distributionRegion.getTotalLevel());
	}
	
	/**
	 * Test a distribution region with two levels level
	 */
	@Test
	public void testTwoLevel() {
		final DistributionRegion distributionRegion = createDistributionGroup(2);
		Assert.assertTrue(distributionRegion.isLeafRegion());
		distributionRegion.setSplit(0);
		Assert.assertTrue(distributionRegion.isLeafRegion());
		
		distributionRegion.getRightChild().setState(DistributionRegionState.ACTIVE);
		distributionRegion.getLeftChild().setState(DistributionRegionState.ACTIVE);

		Assert.assertFalse(distributionRegion.isLeafRegion());

		Assert.assertEquals(distributionRegion, distributionRegion.getLeftChild().getRootRegion());
		Assert.assertEquals(distributionRegion, distributionRegion.getRightChild().getRootRegion());
		
		Assert.assertEquals(distributionRegion.getDimension(), distributionRegion.getLeftChild().getDimension());
		Assert.assertEquals(distributionRegion.getDimension(), distributionRegion.getRightChild().getDimension());
		
		Assert.assertEquals(1, distributionRegion.getLeftChild().getLevel());
		Assert.assertEquals(1, distributionRegion.getRightChild().getLevel());
		
		Assert.assertEquals(2, distributionRegion.getTotalLevel());
	}
	
	/**
	 * Test the split dimension 2d
	 */
	@Test
	public void testSplitDimension1() {
		final DistributionRegion level0 = createDistributionGroup(2);
		level0.setSplit(50);
		final DistributionRegion level1 = level0.getLeftChild();
		level1.setSplit(40);
		final DistributionRegion level2 = level1.getLeftChild();
		level2.setSplit(-30);
		final DistributionRegion level3 = level2.getLeftChild();
		level3.setSplit(30);
		final DistributionRegion level4 = level3.getLeftChild();

		Assert.assertEquals(0, level0.getSplitDimension());
		Assert.assertEquals(1, level1.getSplitDimension());
		Assert.assertEquals(0, level2.getSplitDimension());
		Assert.assertEquals(1, level3.getSplitDimension());
		Assert.assertEquals(0, level4.getSplitDimension());
	}
	
	/**
	 * Test the split dimension 3d
	 */
	@Test
	public void testSplitDimension2() {
		final DistributionRegion level0 = createDistributionGroup(3);
		level0.setSplit(50);
		final DistributionRegion level1 = level0.getLeftChild();
		level1.setSplit(40);
		final DistributionRegion level2 = level1.getLeftChild();
		level2.setSplit(30);
		final DistributionRegion level3 = level2.getLeftChild();
		level3.setSplit(30);
		final DistributionRegion level4 = level3.getLeftChild();

		Assert.assertEquals(0, level0.getSplitDimension());
		Assert.assertEquals(1, level1.getSplitDimension());
		Assert.assertEquals(2, level2.getSplitDimension());
		Assert.assertEquals(0, level3.getSplitDimension());
		Assert.assertEquals(1, level4.getSplitDimension());
		
		Assert.assertEquals(5, level0.getTotalLevel());
		Assert.assertEquals(5, level1.getTotalLevel());
		Assert.assertEquals(5, level2.getTotalLevel());
		Assert.assertEquals(5, level3.getTotalLevel());
		Assert.assertEquals(5, level4.getTotalLevel());
	}
	
	/**
	 * Test isLeftChild and isRightChild method
	 */
	@Test
	public void testLeftOrRightChild() {
		final DistributionRegion level0 = createDistributionGroup(2);
		Assert.assertFalse(level0.isLeftChild());
		Assert.assertFalse(level0.isRightChild());
		
		level0.setSplit(50);
				
		Assert.assertTrue(level0.getLeftChild().isLeftChild());
		Assert.assertTrue(level0.getRightChild().isRightChild());
		Assert.assertFalse(level0.getRightChild().isLeftChild());
		Assert.assertFalse(level0.getLeftChild().isRightChild());
	}

	/**
	 * Test the find systems method
	 */
	@Test
	public void testFindSystems() {
		final BBoxDBInstance SYSTEM_A = new BBoxDBInstance("192.168.1.200:5050");
		final BBoxDBInstance SYSTEM_B = new BBoxDBInstance("192.168.1.201:5050");
		
		final DistributionRegion level0 = createDistributionGroup(1);
		level0.setSplit(50);

		level0.getLeftChild().addSystem(SYSTEM_A);
		level0.getRightChild().addSystem(SYSTEM_B);
		
		level0.setState(DistributionRegionState.SPLIT);
		level0.getLeftChild().setState(DistributionRegionState.ACTIVE);
		level0.getRightChild().setState(DistributionRegionState.ACTIVE);
				
		Assert.assertFalse(convertHopsToSystems(level0.getRoutingHopsForRead(new BoundingBox(100d, 110d))).contains(SYSTEM_A));
		Assert.assertTrue(convertHopsToSystems(level0.getRoutingHopsForRead(new BoundingBox(0d, 10d))).contains(SYSTEM_A));
		
		Assert.assertTrue(convertHopsToSystems(level0.getRoutingHopsForRead(new BoundingBox(0d, 10d))).contains(SYSTEM_A));
		Assert.assertFalse(convertHopsToSystems(level0.getRoutingHopsForRead(new BoundingBox(100d, 110d))).contains(SYSTEM_A));
		
		Assert.assertTrue(convertHopsToSystems(level0.getRoutingHopsForRead(new BoundingBox(0d, 100d))).contains(SYSTEM_A));
		Assert.assertTrue(convertHopsToSystems(level0.getRoutingHopsForRead(new BoundingBox(0d, 100d))).contains(SYSTEM_B));
	}
	
	/**
	 * Convert the routing hop list to distibuted instance list
	 * @param collection
	 * @return
	 */
	protected Collection<BBoxDBInstance> convertHopsToSystems(final Collection<RoutingHop> hops) {
		return hops.stream()
				.map(h -> h.getDistributedInstance())
				.collect(Collectors.toList());
	}
	
	/**
	 * Test name prefix search
	 * @throws InterruptedException 
	 */
	@Test
	public void testNameprefixSearch() throws InterruptedException {
		final DistributionGroupName distributionGroupName = new DistributionGroupName("foo");
		final DistributionRegion level0 = DistributionRegion.createRootElement(distributionGroupName);
		level0.setRegionId(1);
		level0.setSplit(50);
		level0.getLeftChild().setRegionId(2);
		level0.getRightChild().setRegionId(3);
		level0.makeChildsActive();
		
		final DistributionRegion level1 = level0.getLeftChild();
		level1.setSplit(40);
		level1.getLeftChild().setRegionId(4);
		level1.getRightChild().setRegionId(5);
		level1.makeChildsActive();
		
		final DistributionRegion level2 = level1.getLeftChild();
		level2.setSplit(30);
		level2.getLeftChild().setRegionId(6);
		level2.getRightChild().setRegionId(7);
		level2.makeChildsActive();
		
		final DistributionRegion level3 = level2.getLeftChild();
		level3.setSplit(35);
		level3.getLeftChild().setRegionId(8);
		level3.getRightChild().setRegionId(9);
		level3.makeChildsActive();

		Assert.assertTrue(DistributionRegionHelper.getDistributionRegionForNamePrefix(level0, 4711) == null);
		
		Assert.assertEquals(level0, DistributionRegionHelper.getDistributionRegionForNamePrefix(level0, 1));
		Assert.assertEquals(level1.getLeftChild(), DistributionRegionHelper.getDistributionRegionForNamePrefix(level0, 4));
		Assert.assertEquals(level1.getRightChild(), DistributionRegionHelper.getDistributionRegionForNamePrefix(level0, 5));
		Assert.assertEquals(level3.getLeftChild(), DistributionRegionHelper.getDistributionRegionForNamePrefix(level0, 8));
		Assert.assertEquals(level3.getRightChild(), DistributionRegionHelper.getDistributionRegionForNamePrefix(level0, 9));
		
		Assert.assertEquals(null, DistributionRegionHelper.getDistributionRegionForNamePrefix(level3, 1));
	}
	
	
	/**
	 * Get the systems for a distribution group
	 */
	@Test
	public void testGetSystemsForDistributionGroup1() {
		final DistributionRegion level0 = createDistributionGroup(2);
		level0.setSplit(50);
		level0.getLeftChild().setRegionId(2);
		level0.getRightChild().setRegionId(3);
		
		level0.setState(DistributionRegionState.SPLITTING);
		level0.makeChildsActive();
		
		level0.addSystem(new BBoxDBInstance("node1:123"));
		level0.getLeftChild().addSystem(new BBoxDBInstance("node2:123"));
		level0.getRightChild().addSystem(new BBoxDBInstance("node3:123"));
		
		final Collection<RoutingHop> systemsRead = level0.getRoutingHopsForRead(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(3, systemsRead.size());
		
		final Collection<RoutingHop> systemsWrite = level0.getRoutingHopsForWrite(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(2, systemsWrite.size());
	}
	
	/**
	 * Get the systems for a distribution group
	 */
	@Test
	public void testGetSystemsForDistributionGroup2() {
		final DistributionRegion level0 = createDistributionGroup(2);
		level0.setSplit(50);
		level0.getLeftChild().setRegionId(2);
		level0.getRightChild().setRegionId(3);
		
		level0.setState(DistributionRegionState.SPLITTING);
		level0.makeChildsActive();
		
		level0.addSystem(new BBoxDBInstance("node1:123"));
		level0.getLeftChild().addSystem(new BBoxDBInstance("node2:123"));
		level0.getRightChild().addSystem(new BBoxDBInstance("node2:123"));
		
		final Collection<RoutingHop> systemsRead = level0.getRoutingHopsForRead(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(2, systemsRead.size());
		
		final Collection<RoutingHop> systemsWrite = level0.getRoutingHopsForWrite(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(1, systemsWrite.size());
	}
	
	/**
	 * Get the distribution groups for a distribution group
	 */
	@Test
	public void testGetDistributionGroupsForDistributionGroup() {
		final DistributionRegion level0 = createDistributionGroup(2);
		level0.setSplit(50);
		level0.getLeftChild().setRegionId(2);
		level0.getLeftChild().setState(DistributionRegionState.ACTIVE);
		level0.getRightChild().setRegionId(3);
		level0.getRightChild().setState(DistributionRegionState.ACTIVE);

		level0.addSystem(new BBoxDBInstance("node1:123"));
		level0.getLeftChild().addSystem(new BBoxDBInstance("node2:123"));
		level0.getRightChild().addSystem(new BBoxDBInstance("node2:123"));
		
		final Set<DistributionRegion> regions = level0.getDistributionRegionsForBoundingBox(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(3, regions.size());
	}
	
	/**
	 * Test the calculation of the covering box
	 * 
	 *   |          |
	 *   |          |
	 *   |    ul    |     ur
	 * 10|----------+-----------
	 *   |          | 
	 *   |    ll    |     lr
	 *   +----------+-----------
	 *              50
	 */
	@Test
	public void testConveringBox1() {
		final DistributionRegion level0 = createDistributionGroup(2);
		level0.setSplit(50);
		level0.makeChildsActive();
		
		Assert.assertTrue(level0.getLeftChild().getConveringBox().overlaps(new BoundingBox(1.0d, 1.0d, 1.0d, 1.0d)));
		Assert.assertTrue(level0.getLeftChild().getConveringBox().overlaps(new BoundingBox(1.0d, 10.0d, 1.0d, 10.0d)));
		Assert.assertTrue(level0.getLeftChild().getConveringBox().overlaps(new BoundingBox(1.0d, 50.0d, 1.0d, 10.0d)));

		Assert.assertTrue(level0.getLeftChild().getConveringBox().overlaps(new BoundingBox(50.0d, 50.0d, 1.0d, 1.0d)));
		Assert.assertTrue(level0.getLeftChild().getConveringBox().overlaps(new BoundingBox(50.0d, 51.0d, 1.0d, 10.0d)));
		Assert.assertFalse(level0.getLeftChild().getConveringBox().overlaps(new BoundingBox(50.1d, 51.0d, 1.0d, 10.0d)));

		Assert.assertTrue(level0.getRightChild().getConveringBox().overlaps(new BoundingBox(50.1d, 50.1d, 1.0d, 1.0d)));
		Assert.assertTrue(level0.getRightChild().getConveringBox().overlaps(new BoundingBox(50.1d, 51.0d, 1.0d, 10.0d)));
		Assert.assertTrue(level0.getRightChild().getConveringBox().overlaps(new BoundingBox(50d, 51.0d, 1.0d, 10.0d)));
		
		level0.getLeftChild().setSplit(10);
		level0.getLeftChild().makeChildsActive();
		level0.getRightChild().setSplit(10);
		level0.getRightChild().makeChildsActive();
		
		// 2D Regions
		final DistributionRegion ll = level0.getLeftChild().getLeftChild();
		final DistributionRegion ul = level0.getLeftChild().getRightChild();
		final DistributionRegion lr = level0.getRightChild().getLeftChild();
		final DistributionRegion ur = level0.getRightChild().getRightChild();
		
		Assert.assertTrue(ll.getConveringBox().overlaps(new BoundingBox(1d, 1d, 1d, 1d)));
		Assert.assertTrue(ll.getConveringBox().overlaps(new BoundingBox(50d, 52d, 1d, 1d)));
		Assert.assertFalse(ll.getConveringBox().overlaps(new BoundingBox(51d, 52d, 1d, 1d)));
		Assert.assertTrue(ll.getConveringBox().overlaps(new BoundingBox(49.99d, 52d, 1d, 1d)));
		Assert.assertFalse(ll.getConveringBox().overlaps(new BoundingBox(50.1d, 52d, 1d, 1d)));

		Assert.assertFalse(lr.getConveringBox().overlaps(new BoundingBox(1d, 1d, 1d, 1d)));
		Assert.assertTrue(lr.getConveringBox().overlaps(new BoundingBox(50d, 52d, 1d, 1d)));
		Assert.assertTrue(lr.getConveringBox().overlaps(new BoundingBox(51d, 52d, 1d, 1d)));
		Assert.assertTrue(lr.getConveringBox().overlaps(new BoundingBox(49.99d, 52d, 1d, 1d)));
		Assert.assertTrue(lr.getConveringBox().overlaps(new BoundingBox(50.1d, 52d, 1d, 1d)));

		Assert.assertTrue(ll.getConveringBox().overlaps(new BoundingBox(20.0d, 60.0d, 5.0d, 25.0d)));
		Assert.assertTrue(lr.getConveringBox().overlaps(new BoundingBox(20.0d, 60.0d, 5.0d, 25.0d)));
		Assert.assertTrue(ul.getConveringBox().overlaps(new BoundingBox(20.0d, 60.0d, 5.0d, 25.0d)));
		Assert.assertTrue(ur.getConveringBox().overlaps(new BoundingBox(20.0d, 60.0d, 5.0d, 25.0d)));
	}

	/**
	 * @param dimensions 
	 * @return
	 */ 
	private DistributionRegion createDistributionGroup(final int dimensions) {
		final String name = "foo";

		DistributionGroupConfigurationCache.getInstance().clear();
		final DistributionGroupConfiguration config = new DistributionGroupConfiguration();
		config.setDimensions(dimensions);
		DistributionGroupConfigurationCache.getInstance().addNewConfiguration(name, config);
		
		final DistributionRegion level0 = DistributionRegion.createRootElement(new DistributionGroupName(name));
		return level0;
	}
	
}

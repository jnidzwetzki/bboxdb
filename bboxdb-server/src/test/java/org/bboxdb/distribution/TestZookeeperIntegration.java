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
package org.bboxdb.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.ZookeeperBBoxDBInstanceAdapter;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.KDtreeSpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.RegionSplitHelper;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZookeeperIntegration {

	/**
	 * The zookeeper client
	 */
	protected static ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	protected static DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The compare delta
	 */
	private final static double DELTA = 0.0001;

	/**
	 * The name of the test region
	 */
	protected static final String TEST_GROUP = "abc";
	
	@BeforeClass
	public static void before() {
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
	
		final Collection<String> zookeepernodes = configuration.getZookeepernodes();
		final String clustername = configuration.getClustername();

		System.out.println("Zookeeper nodes are: " + zookeepernodes);
		System.out.println("Zookeeper cluster is: " + clustername);
	
		zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		zookeeperClient.init();
		
		distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
	}
	
	@AfterClass
	public static void after() {
		zookeeperClient.shutdown();
	}

	/**
	 * Test the id generation
	 * @throws ZookeeperException
	 */
	@Test
	public void testTableIdGenerator() throws ZookeeperException {
		final List<Integer> ids = new ArrayList<Integer>();
		
		for(int i = 0; i < 10; i++) {
			int nextId = distributionGroupZookeeperAdapter.getNextTableIdForDistributionGroup("mygroup1");
			System.out.println("The next id is: " + nextId);
			
			Assert.assertFalse(ids.contains(nextId));
			ids.add(nextId);
		}
	}
	
	/**
	 * Test the replace with test method
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	@Test
	public void testAndReplaceValue() throws ZookeeperException, ZookeeperNotFoundException {
		final String path = "/testnode";
		zookeeperClient.createPersistentNode(path, "value1".getBytes());
		
		Assert.assertTrue(zookeeperClient.exists(path));
		
		final boolean result1 = zookeeperClient.testAndReplaceValue(path, "value1", "value2");
		Assert.assertTrue(result1);
		Assert.assertEquals("value2", zookeeperClient.readPathAndReturnString(path, true, null));
		
		// Set new value with wrong old value => value should not change
		final boolean result2 = zookeeperClient.testAndReplaceValue(path, "abc", "value3");
		Assert.assertFalse(result2);
		Assert.assertEquals("value2", zookeeperClient.readPathAndReturnString(path, true, null));
		
		zookeeperClient.deleteNodesRecursive(path);
		Assert.assertFalse(zookeeperClient.exists(path));
	}
	
	/**
	 * Test the creation and the deletion of a distribution group
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 */
	@Test
	public void testDistributionGroupCreateDelete() throws ZookeeperException, ZookeeperNotFoundException {
		
		// Create new group
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration()); 
		
		final List<DistributionGroupName> groups = distributionGroupZookeeperAdapter.getDistributionGroups();
		System.out.println(groups);
		boolean found = false;
		for(final DistributionGroupName group : groups) {
			if(group.getFullname().equals(TEST_GROUP)) {
				found = true;
			}
		}
		
		Assert.assertTrue(found);
		Assert.assertTrue(distributionGroupZookeeperAdapter.isDistributionGroupRegistered(TEST_GROUP));
		
		// Delete group
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		final List<DistributionGroupName> groups2 = distributionGroupZookeeperAdapter.getDistributionGroups();
		found = false;
		for(final DistributionGroupName group : groups2) {
			if(group.getFullname().equals(TEST_GROUP)) {
				found = true;
			}
		}
		
		Assert.assertFalse(found);
		Assert.assertFalse(distributionGroupZookeeperAdapter.isDistributionGroupRegistered(TEST_GROUP));
	}
	
	/**
	 * Test the replication factor of a distribution group
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	@Test
	public void testDistributionGroupReplicationFactor() throws ZookeeperException, ZookeeperNotFoundException {
		DistributionGroupConfigurationCache.getInstance().clear();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(TEST_GROUP);

		Assert.assertEquals(3, config.getReplicationFactor());
	}
	
	/**
	 * Test the split of a distribution region
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testDistributionRegionSplitAndMerge() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		// Split and update
		System.out.println("---> Get space partitioner");
		final KDtreeSpacePartitioner spacePartitioner = (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		System.out.println("---> Get space partitioner - DONE");
		final DistributionRegion distributionGroup = spacePartitioner.getRootNode();
		System.out.println("---> Get root node - DONE");

		Assert.assertEquals(TEST_GROUP, distributionGroup.getDistributionGroupName().getFullname());
		
		final DistributionRegionState stateForDistributionRegion1 = distributionGroupZookeeperAdapter.getStateForDistributionRegion(distributionGroup);
		Assert.assertEquals(DistributionRegionState.ACTIVE, stateForDistributionRegion1);
		try {
			System.out.println("--> Set split for node");
			spacePartitioner.splitNode(distributionGroup, 10);
		} catch (ResourceAllocationException e) {
			// Ignore this for the unit test
		}
		
		Thread.sleep(5000);
		Assert.assertEquals(10.0, distributionGroup.getSplit(), DELTA);
		final DistributionRegionState stateForDistributionRegion2 = distributionGroupZookeeperAdapter.getStateForDistributionRegion(distributionGroup);
		Assert.assertEquals(DistributionRegionState.SPLITTING, stateForDistributionRegion2);

		// Reread group from zookeeper
		final DistributionRegion newDistributionGroup = spacePartitioner.getRootNode();
		Assert.assertEquals(10.0, newDistributionGroup.getSplit(), DELTA);
		final DistributionRegionState stateForDistributionRegion3 = distributionGroupZookeeperAdapter.getStateForDistributionRegion(newDistributionGroup);
		Assert.assertEquals(DistributionRegionState.SPLITTING, stateForDistributionRegion3);
		
		// Delete childs
		System.out.println("---> Calling prepare merge");
		spacePartitioner.prepareMerge(spacePartitioner.getRootNode());
		
		// Sleep some seconds to wait for the update
		Thread.sleep(5000);
		
		System.out.println("---> Calling merge complete");
		spacePartitioner.mergeComplete(spacePartitioner.getRootNode());
		final DistributionRegion newDistributionGroup2 = spacePartitioner.getRootNode();
		final DistributionRegionState stateForDistributionRegion4 = distributionGroupZookeeperAdapter.getStateForDistributionRegion(newDistributionGroup2);
		Assert.assertEquals(DistributionRegionState.ACTIVE, stateForDistributionRegion4);
	}
	
	/**
	 * Test the distribution of changes in the zookeeper structure (reading data from the second object)
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testDistributionRegionSplitWithZookeeperPropergate() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final KDtreeSpacePartitioner adapter1 = 
				(KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion distributionGroup1 = adapter1.getRootNode();
		
		final KDtreeSpacePartitioner adapter2 = 
				(KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion distributionGroup2 = adapter2.getRootNode();

		// Update object 1
		try {
			adapter1.splitNode(distributionGroup1, 10);
		} catch (ResourceAllocationException e) {
			// Ignore in unit test
		}
		
		// Sleep some seconds to wait for the update
		Thread.sleep(5000);

		// Read update from the second object
		Assert.assertEquals(10.0, distributionGroup2.getSplit(), DELTA);
		
		// Check region ids
		Assert.assertEquals(0, distributionGroup2.getRegionId());
		Assert.assertEquals(1, distributionGroup2.getLeftChild().getRegionId());
		Assert.assertEquals(2, distributionGroup2.getRightChild().getRegionId());
	}
	
	/**
	 * Test the distribution of changes in the zookeeper structure (reading data from the second object)
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testDistributionRegionSplitWithZookeeperPropergate2() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(2)); 
		
		final KDtreeSpacePartitioner adapter1 
			= (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion distributionGroup1 = adapter1.getRootNode();
		
		final KDtreeSpacePartitioner adapter2 
			= (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion distributionGroup2 = adapter2.getRootNode();

		Assert.assertEquals(0, distributionGroup1.getLevel());
		
		// Update object 1
		try {
			adapter1.splitNode(distributionGroup1, 10);
		} catch (ResourceAllocationException e) {
			// Ignore in unit test
		}
		
		final DistributionRegion leftChild = distributionGroup1.getLeftChild();
		Assert.assertEquals(1, leftChild.getLevel());
		Assert.assertEquals(1, leftChild.getSplitDimension());
		
		try {
			adapter1.splitNode(leftChild, 50);
		} catch (ResourceAllocationException e) {
			// Ignore in unit test
		}

		// Sleep 2 seconds to wait for the update
		Thread.sleep(2000);

		// Read update from the second object
		Assert.assertEquals(10.0, distributionGroup2.getSplit(), DELTA);
		Assert.assertEquals(50.0, distributionGroup2.getLeftChild().getSplit(), DELTA);
	}
	
	/**
	 * Test the system register and unregister methods
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	@Test
	public void testSystemRegisterAndUnregister() throws ZookeeperException, ZookeeperNotFoundException {
		final BBoxDBInstance systemName = new BBoxDBInstance("192.168.1.10:5050");
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final DistributionRegion region = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP).getRootNode();
		final Collection<BBoxDBInstance> systems1 = distributionGroupZookeeperAdapter.getSystemsForDistributionRegion(region, null);
		Assert.assertEquals(0, systems1.size());
		
		// Add a system
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, systemName);
		final Collection<BBoxDBInstance> systems2 = distributionGroupZookeeperAdapter.getSystemsForDistributionRegion(region, null);
		Assert.assertEquals(1, systems2.size());
		Assert.assertTrue(systems2.contains(systemName));
		
		distributionGroupZookeeperAdapter.deleteSystemFromDistributionRegion(region, systemName);
		final Collection<BBoxDBInstance> systems3 = distributionGroupZookeeperAdapter.getSystemsForDistributionRegion(region, null);
		Assert.assertEquals(0, systems3.size());
	}
	
	/**
	 * Test the statistics for a given region
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	@Test
	public void testStatistics1() throws ZookeeperException, ZookeeperNotFoundException {
		final BBoxDBInstance system1 = new BBoxDBInstance("192.168.1.10:5050");
		final BBoxDBInstance system2 = new BBoxDBInstance("192.168.1.11:5050");

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		final DistributionRegion region = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP).getRootNode();

		final RegionSplitHelper regionSplitHelper = new RegionSplitHelper();
		final double size1 = regionSplitHelper.getMaxRegionSizeFromStatistics(region);
		Assert.assertEquals(0, size1, DELTA);
		
		final Map<BBoxDBInstance, Map<String, Long>> statistics1 = distributionGroupZookeeperAdapter.getRegionStatistics(region);
		Assert.assertTrue(statistics1.isEmpty());
		
		distributionGroupZookeeperAdapter.updateRegionStatistics(region, system1, 12, 999);
		final Map<BBoxDBInstance, Map<String, Long>> statistics2 = distributionGroupZookeeperAdapter.getRegionStatistics(region);
		Assert.assertEquals(1, statistics2.size());
		Assert.assertEquals(12, statistics2.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE).longValue());
		Assert.assertEquals(999, statistics2.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES).longValue());
		final double size2 = regionSplitHelper.getMaxRegionSizeFromStatistics(region);
		Assert.assertEquals(12, size2, DELTA);
		
		distributionGroupZookeeperAdapter.updateRegionStatistics(region, system2, 33, 1234);
		final Map<BBoxDBInstance, Map<String, Long>> statistics3 = distributionGroupZookeeperAdapter.getRegionStatistics(region);
		Assert.assertEquals(2, statistics3.size());
		Assert.assertEquals(12, statistics3.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE).longValue());
		Assert.assertEquals(999, statistics3.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES).longValue());
		Assert.assertEquals(33, statistics3.get(system2).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE).longValue());
		Assert.assertEquals(1234, statistics3.get(system2).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES).longValue());
		final double size3 = regionSplitHelper.getMaxRegionSizeFromStatistics(region);
		Assert.assertEquals(33, size3, DELTA);
	}
	
	/**
	 * Test the statistics of child regions
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testStatistics2() throws ZookeeperException, InterruptedException {
		final BBoxDBInstance system1 = new BBoxDBInstance("192.168.1.10:5050");

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		final DistributionRegion region = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP).getRootNode();

		final RegionSplitHelper regionSplitHelper = new RegionSplitHelper();
		final double totalSize1 = regionSplitHelper.getTotalRegionSize(region);
		Assert.assertEquals(0, totalSize1, DELTA);
		
		region.setSplit(12);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));
		
		distributionGroupZookeeperAdapter.updateRegionStatistics(region.getLeftChild(), system1, 12, 999);
		distributionGroupZookeeperAdapter.updateRegionStatistics(region.getRightChild(), system1, 33, 999);

		final double totalSize2 = regionSplitHelper.getTotalRegionSize(region);
		Assert.assertEquals(45, totalSize2, DELTA);
	}
	
	/**
	 * Test the set and get checkpoint methods
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSystemCheckpoint1() throws ZookeeperException, InterruptedException {
		final BBoxDBInstance systemName1 = new BBoxDBInstance("192.168.1.10:5050");
		final BBoxDBInstance systemName2 = new BBoxDBInstance("192.168.1.20:5050");

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final DistributionRegion region = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP).getRootNode();

		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, systemName1);
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, systemName2);

		final long checkpoint1 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region, systemName1);
		Assert.assertEquals(-1, checkpoint1);

		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region, systemName1, 5000);
		final long checkpoint2 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region, systemName1);
		Assert.assertEquals(5000, checkpoint2);
		
		// System 2
		final long checkpoint3 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(-1, checkpoint3);

		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region, systemName2, 9000);
		final long checkpoint4 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(9000, checkpoint4);
		
		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region, systemName2, 9001);
		final long checkpoint5 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(9001, checkpoint5);
	}
	
	/**
	 * Test the set and get checkpoint methods
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testSystemCheckpoint2() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		final BBoxDBInstance systemName1 = new BBoxDBInstance("192.168.1.10:5050");
		final BBoxDBInstance systemName2 = new BBoxDBInstance("192.168.1.20:5050");

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final KDtreeSpacePartitioner distributionGroupAdapter 
			= (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion region = distributionGroupAdapter.getRootNode();
		try {
			distributionGroupAdapter.splitNode(region, 50);
		} catch (ResourceAllocationException e) {
			// Ignore in unit test
		}
		
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region.getLeftChild(), systemName1);
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region.getLeftChild(), systemName2);
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region.getRightChild(), systemName1);
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region.getRightChild(), systemName2);
		
		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region.getLeftChild(), systemName1, 1);
		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region.getLeftChild(), systemName2, 2);
		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region.getRightChild(), systemName1, 3);
		distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(region.getRightChild(), systemName2, 4);

		final long checkpoint1 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region.getLeftChild(), systemName1);
		final long checkpoint2 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region.getLeftChild(), systemName2);
		final long checkpoint3 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region.getRightChild(), systemName1);
		final long checkpoint4 = distributionGroupZookeeperAdapter.getCheckpointForDistributionRegion(region.getRightChild(), systemName2);

		Assert.assertEquals(1, checkpoint1);
		Assert.assertEquals(2, checkpoint2);
		Assert.assertEquals(3, checkpoint3);
		Assert.assertEquals(4, checkpoint4);
	}
	
	/**
	 * Test the systems field
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSystems() throws ZookeeperException, InterruptedException {
		final BBoxDBInstance systemName = new BBoxDBInstance("192.168.1.10:5050");
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final DistributionRegion region = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP).getRootNode();
		distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, systemName);
		
		// Sleep 2 seconds to wait for the update
		Thread.sleep(2000);

		Assert.assertEquals(1, region.getSystems().size());
		Assert.assertTrue(region.getSystems().contains(systemName));
	}

	/**
	 * Test the generation of the nameprefix
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testNameprefix1() throws ZookeeperException, InterruptedException {
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final DistributionRegion region = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP).getRootNode();
		Assert.assertEquals(0, region.getRegionId());
	}
	
	/**
	 * Test the generation of the nameprefix
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testNameprefix2() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
 		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final KDtreeSpacePartitioner distributionGroupAdapter 
			= (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion region = distributionGroupAdapter.getRootNode();
		
		try {
			distributionGroupAdapter.splitNode(region, 10);
		} catch (ResourceAllocationException e) {
			// Ignore systems exception
		}
		
		final DistributionRegion leftChild = region.getLeftChild();
		final DistributionRegion rightChild = region.getRightChild();

		// No systems can be assigned, so the nodes remain in CREATING state
		Assert.assertEquals(DistributionRegionState.CREATING, leftChild.getState());
		Assert.assertEquals(DistributionRegionState.CREATING, rightChild.getState());
		
		Assert.assertEquals(0, region.getRegionId());
		Assert.assertEquals(1, leftChild.getRegionId());
		Assert.assertEquals(2, rightChild.getRegionId());
	}
	
	/**
	 * Test the path decoding and encoding
	 * @throws ZookeeperException 
	 */
	@Test
	public void testPathDecodeAndEncode() throws ZookeeperException {

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(2)); 
		
		final DistributionGroupName distributionGroupName = new DistributionGroupName(TEST_GROUP);
		final DistributionRegion level0 = DistributionRegion.createRootElement(distributionGroupName);
		level0.setRegionId(1);
		level0.setSplit(50);
		level0.makeChildsActive();

		// Level 1
		final DistributionRegion level1l = level0.getLeftChild();
		level1l.setSplit(40);
		level1l.makeChildsActive();
		
		final DistributionRegion level1r = level0.getRightChild();
		level1r.setSplit(50);
		level1r.makeChildsActive();

		// Level 2
		final DistributionRegion level2ll = level1l.getLeftChild();
		level2ll.setSplit(30);
		level2ll.makeChildsActive();

		final DistributionRegion level2rl = level1r.getLeftChild();
		level2rl.setSplit(60);
		level2rl.makeChildsActive();

		final DistributionRegion level2lr = level1l.getRightChild();
		level2lr.setSplit(30);
		level2lr.makeChildsActive();

		final DistributionRegion level2rr = level1r.getRightChild();
		level2rr.setSplit(60);
		level2rr.makeChildsActive();


		// Level 3
		final DistributionRegion level3lll = level2ll.getLeftChild();
		level3lll.setSplit(35);
		level3lll.makeChildsActive();


		final DistributionGroupZookeeperAdapter zookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
		
		final String path0 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level0);
		final String path1 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level1l);
		final String path2 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level1r);
		final String path3 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level2ll);
		final String path4 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level2rl);
		final String path5 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level2lr);
		final String path6 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level2rr);
		final String path7 = zookeeperAdapter.getZookeeperPathForDistributionRegion(level3lll);
		
		Assert.assertEquals(level0, distributionGroupZookeeperAdapter.getNodeForPath(level0, path0));
		Assert.assertEquals(level1l, distributionGroupZookeeperAdapter.getNodeForPath(level0, path1));
		Assert.assertEquals(level1r, distributionGroupZookeeperAdapter.getNodeForPath(level0, path2));
		Assert.assertEquals(level2ll, distributionGroupZookeeperAdapter.getNodeForPath(level0, path3));
		Assert.assertEquals(level2rl, distributionGroupZookeeperAdapter.getNodeForPath(level0, path4));
		Assert.assertEquals(level2lr, distributionGroupZookeeperAdapter.getNodeForPath(level0, path5));
		Assert.assertEquals(level2rr, distributionGroupZookeeperAdapter.getNodeForPath(level0, path6));
		Assert.assertEquals(level3lll, distributionGroupZookeeperAdapter.getNodeForPath(level0, path7));
	}
	
	
	/**
	 * Test the deletion and the creation of an instance
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test
	public void testCreateAndDeleteDistributionGroup() 
			throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		final KDtreeSpacePartitioner cacheGroup 
			= (KDtreeSpacePartitioner) SpacePartitionerCache.getSpacePartitionerForGroupName(TEST_GROUP);
		Assert.assertTrue(cacheGroup.getRootNode().isLeafRegion());

		System.out.println("---> Delete");
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		System.out.println("---> Create");

		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(1)); 
		
		System.out.println("---> Split");

		final KDtreeSpacePartitioner kdTreeAdapter 
			= (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		
		try {
			kdTreeAdapter.splitNode(kdTreeAdapter.getRootNode(), (float) 20.0);
		} catch (ResourceAllocationException e) {
			// Ignore in unit test
		} 
				
		System.out.println("---> Split done");

		// Test fresh copy
		final SpacePartitioner freshGroup = distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		Assert.assertEquals((float) 20.0, freshGroup.getRootNode().getSplit(), 0.00001);
		
		// Test cached instance
		Assert.assertEquals((float) 20.0, cacheGroup.getRootNode().getSplit(), 0.00001);	
	}
	
	/**
	 * Test the path quoting
	 */
	@Test
	public void testPathQuoting() {
		final String path1 = "/tmp";
		final String path2 = "/etc/init.d";
		final String path3 = "/tmp/test_abc";
		
		final List<String> paths = Arrays.asList(path1, path2, path3);
		
		for(final String path : paths) {
			Assert.assertTrue(path.contains("/"));
			final String quotedPath = ZookeeperBBoxDBInstanceAdapter.quotePath(path);
			Assert.assertFalse(quotedPath.contains("/"));
			final String unquotedPath = ZookeeperBBoxDBInstanceAdapter.unquotePath(quotedPath);
			Assert.assertEquals(path, unquotedPath);
		}
		
	}
}

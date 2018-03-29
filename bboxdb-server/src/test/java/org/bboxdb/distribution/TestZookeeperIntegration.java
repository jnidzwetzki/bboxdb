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
import java.util.OptionalDouble;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.ZookeeperInstancePathHelper;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.KDtreeSpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.RegionMergeHelper;
import org.bboxdb.distribution.partitioner.regionsplit.StatisticsHelper;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZookeeperIntegration {

	/**
	 * The zookeeper client
	 */
	private static ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The region adapter
	 */
	private static DistributionRegionAdapter distributionRegionAdapter;
	
	/**
	 * The compare delta
	 */
	private final static double DELTA = 0.0001;

	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "abc";
	
	@BeforeClass
	public static void beforeClass() {
		zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		
		distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		
		distributionRegionAdapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();	
	}
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException {
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withReplicationFactor((short) 1)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
		
		Assert.assertTrue(zookeeperClient.getClustername().length() > 5);
	}

	/**
	 * Test the id generation
	 * @throws ZookeeperException
	 */
	@Test(timeout=60000)
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
	@Test(timeout=60000)
	public void testAndReplaceValue() throws ZookeeperException, ZookeeperNotFoundException {
		final String path = "/testnode";
		zookeeperClient.createPersistentNode(path, "value1".getBytes());
		
		Assert.assertTrue(zookeeperClient.exists(path));
		
		final boolean result1 = zookeeperClient.testAndReplaceValue(path, "value1", "value2");
		Assert.assertTrue(result1);
		Assert.assertEquals("value2", zookeeperClient.readPathAndReturnString(path));
		
		// Set new value with wrong old value => value should not change
		final boolean result2 = zookeeperClient.testAndReplaceValue(path, "abc", "value3");
		Assert.assertFalse(result2);
		Assert.assertEquals("value2", zookeeperClient.readPathAndReturnString(path));
		
		zookeeperClient.deleteNodesRecursive(path);
		Assert.assertFalse(zookeeperClient.exists(path));
	}
	
	/**
	 * Test the creation and the deletion of a distribution group
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testDistributionGroupCreateDelete() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		final List<String> groups = distributionGroupZookeeperAdapter.getDistributionGroups();
		System.out.println(groups);
		Assert.assertTrue(groups.contains(TEST_GROUP));
		Assert.assertTrue(distributionGroupZookeeperAdapter.isDistributionGroupRegistered(TEST_GROUP));
		
		// Delete group
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		final List<String> groups2 = distributionGroupZookeeperAdapter.getDistributionGroups();
		Assert.assertFalse(groups2.contains(TEST_GROUP));
		Assert.assertFalse(distributionGroupZookeeperAdapter.isDistributionGroupRegistered(TEST_GROUP));
	}
	
	/** 
	 * Test the table deletion and creation
	 * @throws ZookeeperException 
	 * @throws BBoxDBException 
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testTableCreateDelete() throws Exception {
		final TupleStoreAdapter tupleStoreAdapter = zookeeperClient.getTupleStoreAdapter();
		final SpacePartitioner spacePartitioner = getSpacePartitioner();
		final DistributionRegion rootNode = spacePartitioner.getRootNode();
		
		final String rootPath = distributionRegionAdapter.getZookeeperPathForDistributionRegion(rootNode);
		distributionRegionAdapter.addSystemToDistributionRegion(rootPath, ZookeeperClientFactory.getLocalInstanceName());

		final TupleStoreName tupleStoreName = new TupleStoreName(TEST_GROUP + "_tabletest");
		final TupleStoreName tupleStoreName0 = new TupleStoreName(TEST_GROUP + "_tabletest_0");

		final TupleStoreConfiguration configuration = TupleStoreConfigurationBuilder.create().build();
		
		tupleStoreAdapter.writeTuplestoreConfiguration(tupleStoreName, configuration);
		
		final TupleStoreManagerRegistry storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
		storageRegistry.deleteTable(tupleStoreName0, true);
		storageRegistry.createTable(tupleStoreName0, configuration);
		
		storageRegistry.getTupleStoreManager(tupleStoreName0);
		Assert.assertTrue(storageRegistry.isStorageManagerKnown(tupleStoreName0));
		
		System.out.println("=== Executing deletion");
		tupleStoreAdapter.deleteTable(tupleStoreName0);
		Thread.sleep(5000);

		Assert.assertFalse(storageRegistry.isStorageManagerKnown(tupleStoreName0));
		storageRegistry.shutdown();
	}
	
	/**
	 * Test the replication factor of a distribution group
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testDistributionGroupReplicationFactor() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
				.getInstance().getDistributionGroupConfiguration(TEST_GROUP);

		Assert.assertEquals(1, config.getReplicationFactor());
	}
	
	
	/**
	 * Test the system register and unregister methods
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testSystemRegisterAndUnregister() throws ZookeeperException, 
		ZookeeperNotFoundException, BBoxDBException {
		
		final BBoxDBInstance systemName = new BBoxDBInstance("192.168.1.10:5050");
		
		final DistributionRegion region = getSpacePartitioner().getRootNode();
		final Collection<BBoxDBInstance> systems1 = distributionRegionAdapter.getSystemsForDistributionRegion(region);
		Assert.assertEquals(1, systems1.size());
		
		// Add a system
		final String path = distributionRegionAdapter.getZookeeperPathForDistributionRegion(region);
		distributionRegionAdapter.addSystemToDistributionRegion(path, systemName);
		final Collection<BBoxDBInstance> systems2 = distributionRegionAdapter.getSystemsForDistributionRegion(region);
		Assert.assertEquals(2, systems2.size());
		Assert.assertTrue(systems2.contains(systemName));
		
		distributionRegionAdapter.deleteSystemFromDistributionRegion(region, systemName);
		final Collection<BBoxDBInstance> systems3 = distributionRegionAdapter.getSystemsForDistributionRegion(region);
		Assert.assertEquals(1, systems3.size());
	}
	
	/**
	 * Test the statistics for a given region
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testStatistics1() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		final BBoxDBInstance system1 = new BBoxDBInstance("192.168.1.10:5050");
		final BBoxDBInstance system2 = new BBoxDBInstance("192.168.1.11:5050");

		final DistributionRegion region = getSpacePartitioner().getRootNode();

		final OptionalDouble size1 = StatisticsHelper.getAndUpdateStatistics(region);
		Assert.assertFalse(size1.isPresent());
		
		final Map<BBoxDBInstance, Map<String, Long>> statistics1 = distributionRegionAdapter.getRegionStatistics(region);
		Assert.assertTrue(statistics1.isEmpty());
		
		distributionRegionAdapter.updateRegionStatistics(region, system1, 12, 999);
		final Map<BBoxDBInstance, Map<String, Long>> statistics2 = distributionRegionAdapter.getRegionStatistics(region);
		Assert.assertEquals(1, statistics2.size());
		Assert.assertEquals(12, statistics2.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE).longValue());
		Assert.assertEquals(999, statistics2.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES).longValue());
		final OptionalDouble size2 = StatisticsHelper.getAndUpdateStatistics(region);
		Assert.assertEquals(12, size2.getAsDouble(), DELTA);
		
		distributionRegionAdapter.updateRegionStatistics(region, system2, 33, 1234);
		final Map<BBoxDBInstance, Map<String, Long>> statistics3 = distributionRegionAdapter.getRegionStatistics(region);
		Assert.assertEquals(2, statistics3.size());
		Assert.assertEquals(12, statistics3.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE).longValue());
		Assert.assertEquals(999, statistics3.get(system1).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES).longValue());
		Assert.assertEquals(33, statistics3.get(system2).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE).longValue());
		Assert.assertEquals(1234, statistics3.get(system2).get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES).longValue());
		final OptionalDouble size3 = StatisticsHelper.getAndUpdateStatistics(region);
		Assert.assertEquals(33, size3.getAsDouble(), DELTA);
	}
	
	/**
	 * Test the statistics of child regions
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void testStatistics2() throws Exception {
		final BBoxDBInstance system1 = new BBoxDBInstance("192.168.1.10:5050");

		final KDtreeSpacePartitioner spaceparitioner = (KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion region = spaceparitioner.getRootNode();

		final OptionalDouble totalSize1 = RegionMergeHelper.getTotalRegionSize(region.getDirectChildren());
		Assert.assertFalse(totalSize1.isPresent());
		
		spaceparitioner.splitNode(region, 12);
		spaceparitioner.waitForSplitCompleteZookeeperCallback(region, 2);
		
		distributionRegionAdapter.updateRegionStatistics(region.getDirectChildren().get(0), system1, 12, 999);
		distributionRegionAdapter.updateRegionStatistics(region.getDirectChildren().get(1), system1, 33, 999);

		StatisticsHelper.clearHistory();
		final OptionalDouble totalSizeAfterClear = RegionMergeHelper.getTotalRegionSize(region.getDirectChildren());
		Assert.assertFalse(totalSizeAfterClear.isPresent());

		// Update complete history
		for(int i = 0; i < StatisticsHelper.HISTORY_LENGTH; i++) {
			RegionMergeHelper.getTotalRegionSize(region.getDirectChildren());
		}
		
		final OptionalDouble totalSize2 = RegionMergeHelper.getTotalRegionSize(region.getDirectChildren());
		Assert.assertEquals(45, totalSize2.getAsDouble(), DELTA);
	}
	
	/**
	 * Test the set and get checkpoint methods
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testSystemCheckpoint1() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		final BBoxDBInstance systemName1 = new BBoxDBInstance("192.168.1.10:5050");
		final BBoxDBInstance systemName2 = new BBoxDBInstance("192.168.1.20:5050");

		final DistributionRegion region = getSpacePartitioner().getRootNode();
		final String path = distributionRegionAdapter.getZookeeperPathForDistributionRegion(region);

		distributionRegionAdapter.addSystemToDistributionRegion(path, systemName1);
		distributionRegionAdapter.addSystemToDistributionRegion(path, systemName2);

		final long checkpoint1 = distributionRegionAdapter.getCheckpointForDistributionRegion(region, systemName1);
		Assert.assertEquals(-1, checkpoint1);

		distributionRegionAdapter.setCheckpointForDistributionRegion(region, systemName1, 5000);
		final long checkpoint2 = distributionRegionAdapter.getCheckpointForDistributionRegion(region, systemName1);
		Assert.assertEquals(5000, checkpoint2);
		
		// System 2
		final long checkpoint3 = distributionRegionAdapter.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(-1, checkpoint3);

		distributionRegionAdapter.setCheckpointForDistributionRegion(region, systemName2, 9000);
		final long checkpoint4 = distributionRegionAdapter.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(9000, checkpoint4);
		
		distributionRegionAdapter.setCheckpointForDistributionRegion(region, systemName2, 9001);
		final long checkpoint5 = distributionRegionAdapter.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(9001, checkpoint5);
	}
	
	/**
	 * Test the set and get checkpoint methods
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void testSystemCheckpoint2() throws Exception {
		final BBoxDBInstance systemName1 = new BBoxDBInstance("192.168.1.10:5050");
		final BBoxDBInstance systemName2 = new BBoxDBInstance("192.168.1.20:5050");

		final KDtreeSpacePartitioner distributionGroupAdapter 
			= (KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion region = distributionGroupAdapter.getRootNode();
		distributionGroupAdapter.splitNode(region, 50);
		
		final DistributionRegion region1 = region.getDirectChildren().get(0);
		final String path1 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(region1);
		final DistributionRegion region2 = region.getDirectChildren().get(1);
		final String path2 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(region2);

		distributionRegionAdapter.addSystemToDistributionRegion(path1, systemName1);
		distributionRegionAdapter.addSystemToDistributionRegion(path1, systemName2);
		distributionRegionAdapter.addSystemToDistributionRegion(path2, systemName1);
		distributionRegionAdapter.addSystemToDistributionRegion(path2, systemName2);
		
		distributionRegionAdapter.setCheckpointForDistributionRegion(region1, systemName1, 1);
		distributionRegionAdapter.setCheckpointForDistributionRegion(region1, systemName2, 2);
		distributionRegionAdapter.setCheckpointForDistributionRegion(region2, systemName1, 3);
		distributionRegionAdapter.setCheckpointForDistributionRegion(region2, systemName2, 4);

		final long checkpoint1 = distributionRegionAdapter.getCheckpointForDistributionRegion(region1, systemName1);
		final long checkpoint2 = distributionRegionAdapter.getCheckpointForDistributionRegion(region1, systemName2);
		final long checkpoint3 = distributionRegionAdapter.getCheckpointForDistributionRegion(region2, systemName1);
		final long checkpoint4 = distributionRegionAdapter.getCheckpointForDistributionRegion(region2, systemName2);

		Assert.assertEquals(1, checkpoint1);
		Assert.assertEquals(2, checkpoint2);
		Assert.assertEquals(3, checkpoint3);
		Assert.assertEquals(4, checkpoint4);
	}
	
	/**
	 * Test the systems field
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testSystems() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		final BBoxDBInstance systemName = new BBoxDBInstance("192.168.1.10:5050");
		
		final DistributionRegion region = getSpacePartitioner().getRootNode();
		final String path = distributionRegionAdapter.getZookeeperPathForDistributionRegion(region);
		Assert.assertEquals(1, region.getSystems().size());

		distributionRegionAdapter.addSystemToDistributionRegion(path, systemName);
		
		// Sleep 2 seconds to wait for the update
		Thread.sleep(2000);

		Assert.assertEquals(2, region.getSystems().size());
		Assert.assertTrue(region.getSystems().contains(systemName));
	}

	/**
	 * Test the generation of the nameprefix
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testNameprefix1() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		final DistributionRegion region = getSpacePartitioner().getRootNode();
		Assert.assertEquals(0, region.getRegionId());
	}
	
	/**
	 * Test the generation of the nameprefix
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void testNameprefix2() throws Exception{
		final KDtreeSpacePartitioner distributionGroupAdapter = (KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion region = distributionGroupAdapter.getRootNode();
		
		distributionGroupAdapter.splitNode(region, 10);
		
		final DistributionRegion leftChild = region.getDirectChildren().get(0);
		final DistributionRegion rightChild = region.getDirectChildren().get(1);

		Assert.assertEquals(DistributionRegionState.REDISTRIBUTION_ACTIVE, leftChild.getState());
		Assert.assertEquals(DistributionRegionState.REDISTRIBUTION_ACTIVE, rightChild.getState());
		
		Assert.assertEquals(0, region.getRegionId());
		Assert.assertEquals(1, leftChild.getRegionId());
		Assert.assertEquals(2, rightChild.getRegionId());
	}
	
	/**
	 * Test the path decoding and encoding
	 * @throws ZookeeperException 
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ResourceAllocationException 
	 */
	@Test(timeout=60000)
	public void testPathDecodeAndEncode() throws ZookeeperException, BBoxDBException, 
		ZookeeperNotFoundException, ResourceAllocationException {

		final KDtreeSpacePartitioner spacepartitionier = (KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion level0 = spacepartitionier.getRootNode();
		
		spacepartitionier.splitNode(level0, 50);	
		level0.makeChildsActive();

		// Level 1
		final DistributionRegion level1l = level0.getDirectChildren().get(0);
		spacepartitionier.splitNode(level1l, 40);
		level1l.makeChildsActive();
		
		final DistributionRegion level1r = level0.getDirectChildren().get(1);
		spacepartitionier.splitNode(level1r, 50);	
		level1r.makeChildsActive();

		// Level 2
		final DistributionRegion level2ll = level1l.getDirectChildren().get(0);
		spacepartitionier.splitNode(level2ll, 30);	
		level2ll.makeChildsActive();

		final DistributionRegion level2rl = level1r.getDirectChildren().get(0);
		spacepartitionier.splitNode(level2rl, 60);
		level2rl.makeChildsActive();

		final DistributionRegion level2lr = level1l.getDirectChildren().get(1);
		spacepartitionier.splitNode(level2lr, 30);
		level2lr.makeChildsActive();

		final DistributionRegion level2rr = level1r.getDirectChildren().get(1);
		spacepartitionier.splitNode(level2rr, 60);
		level2rr.makeChildsActive();
		
		// Level 3
		final DistributionRegion level3lll = level2ll.getDirectChildren().get(0);
		spacepartitionier.splitNode(level3lll, 35);
		level3lll.makeChildsActive();
		
		final String path0 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level0);
		final String path1 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level1l);
		final String path2 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level1r);
		final String path3 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level2ll);
		final String path4 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level2rl);
		final String path5 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level2lr);
		final String path6 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level2rr);
		final String path7 = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level3lll);
		
		Assert.assertEquals(level0, distributionRegionAdapter.getNodeForPath(level0, path0));
		Assert.assertEquals(level1l, distributionRegionAdapter.getNodeForPath(level0, path1));
		Assert.assertEquals(level1r, distributionRegionAdapter.getNodeForPath(level0, path2));
		Assert.assertEquals(level2ll, distributionRegionAdapter.getNodeForPath(level0, path3));
		Assert.assertEquals(level2rl, distributionRegionAdapter.getNodeForPath(level0, path4));
		Assert.assertEquals(level2lr, distributionRegionAdapter.getNodeForPath(level0, path5));
		Assert.assertEquals(level2rr, distributionRegionAdapter.getNodeForPath(level0, path6));
		Assert.assertEquals(level3lll, distributionRegionAdapter.getNodeForPath(level0, path7));
	}
	
	
	/**
	 * Test the deletion and the creation of an instance
	 * @throws Exception 
	 */
	@Test(timeout=60000)
	public void testCreateAndDeleteDistributionGroup() throws Exception {
		
		final KDtreeSpacePartitioner cacheGroup 
			= (KDtreeSpacePartitioner) SpacePartitionerCache.getInstance()
			.getSpacePartitionerForGroupName(TEST_GROUP);
		
		Assert.assertTrue(cacheGroup.getRootNode().isLeafRegion());

		System.out.println("---> Split");

		final KDtreeSpacePartitioner kdTreeAdapter = (KDtreeSpacePartitioner) getSpacePartitioner();
		
		kdTreeAdapter.splitNode(kdTreeAdapter.getRootNode(), (float) 20.0);
				
		System.out.println("---> Split done");

		// Test fresh copy
		final SpacePartitioner freshGroup = getSpacePartitioner();
		final DistributionRegion firstChildFresh = freshGroup.getRootNode().getDirectChildren().get(0);
		Assert.assertEquals(20.0, firstChildFresh.getConveringBox().getCoordinateHigh(0), DELTA);
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		
		// Test cached instance
		try {
			cacheGroup.getRootNode().getDirectChildren().get(0);
			
			// This should not happen
			Assert.assertFalse(true);
		} catch (BBoxDBException e) {
			// Unable to get root on a space partitioner after shutdown
		}
	}

	/**
	 * Get a new space partitioner instance
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	private SpacePartitioner getSpacePartitioner() throws ZookeeperException, ZookeeperNotFoundException,
		BBoxDBException {
		
		return SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP);
	}
	
	/**
	 * Test the path quoting
	 */
	@Test(timeout=60000)
	public void testPathQuoting() {
		final String path1 = "/tmp";
		final String path2 = "/etc/init.d";
		final String path3 = "/tmp/test_abc";
		
		final List<String> paths = Arrays.asList(path1, path2, path3);
		
		for(final String path : paths) {
			Assert.assertTrue(path.contains("/"));
			final String quotedPath = ZookeeperInstancePathHelper.quotePath(path);
			Assert.assertFalse(quotedPath.contains("/"));
			final String unquotedPath = ZookeeperInstancePathHelper.unquotePath(quotedPath);
			Assert.assertEquals(path, unquotedPath);
		}
	}
	
	/**
	 * Test merging supported
	 * @throws ZookeeperException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testMergingSupported() throws ZookeeperException, BBoxDBException {
		final SpacePartitioner spacePartitioner = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(TEST_GROUP);
				
		final DistributionRegion rootNode = spacePartitioner.getRootNode();
		Assert.assertTrue(distributionRegionAdapter.isMergingSupported(rootNode));
		
		distributionRegionAdapter.setMergingSupported(rootNode, false);
		Assert.assertFalse(distributionRegionAdapter.isMergingSupported(rootNode));

		distributionRegionAdapter.setMergingSupported(rootNode, true);
		Assert.assertTrue(distributionRegionAdapter.isMergingSupported(rootNode));
	}
	
	/**
	 * Test the reading and the writing of the distribution group configuration
	 * @throws ZookeeperException 
	 * @throws InputParseException 
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testDistributionGroupConfiguration() throws ZookeeperException, ZookeeperNotFoundException, InputParseException, BBoxDBException {
		final DistributionGroupConfiguration configuration = new DistributionGroupConfiguration(45);
		configuration.setMaximumRegionSize(342);
		configuration.setMinimumRegionSize(53454);
		configuration.setPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy");
		configuration.setPlacementStrategyConfig("def");
		configuration.setReplicationFactor((short) 99);
		configuration.setSpacePartitioner(Const.DEFAULT_SPACE_PARTITIONER);
		configuration.setSpacePartitionerConfig("xyz");
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
		
		final DistributionGroupConfiguration readConfiguration 
			= distributionGroupZookeeperAdapter.getDistributionGroupConfiguration(TEST_GROUP);
		
		Assert.assertEquals(configuration, readConfiguration);
	}
}

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
package org.bboxdb.distribution.partition;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.QuadtreeSpacePartitioner;
import org.bboxdb.distribution.partitioner.regionsplit.RegionMergeHelper;
import org.bboxdb.distribution.partitioner.regionsplit.RegionSplitHelper;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQuadtreeSpacePartitioner {
	
	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "abc";
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;
	
	@BeforeClass
	public static void beforeClass() {
		distributionGroupZookeeperAdapter 
		= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
	}
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException {
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.QuadtreeSpacePartitioner", "")
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}

	@Test(timeout=60000)
	public void testSplit0() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, ResourceAllocationException {
		
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		
		Assert.assertEquals(0, rootNode.getDirectChildren().size());
		spacepartitionier.splitRegion(rootNode, new HashSet<>());
		Assert.assertEquals(4, rootNode.getDirectChildren().size());
	}
	
	@Test(timeout=60000)
	public void testSplit1() throws ZookeeperException, ZookeeperNotFoundException, 
		BBoxDBException, ResourceAllocationException, InterruptedException {
		
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		
		Assert.assertEquals(0, rootNode.getDirectChildren().size());
		spacepartitionier.splitRegion(rootNode, new HashSet<>());
		Assert.assertEquals(4, rootNode.getDirectChildren().size());
		
		System.out.println("=== Split region");
		final DistributionRegion child1 = rootNode.getChildNumber(1);
		final List<DistributionRegion> destination = spacepartitionier.splitRegion(child1, new HashSet<>());
		Assert.assertEquals(4, child1.getDirectChildren().size());
		Assert.assertEquals(8, rootNode.getAllChildren().size());		
		
		final DistributionRegionSyncer distributionRegionSyncer 
			= spacepartitionier.getDistributionRegionSyncer();
		
		System.out.println("=== Split failed");
		final CountDownLatch changeLatch = new CountDownLatch(1);
		
		final DistributionRegionCallback callback = (e, r) -> { 
			if(rootNode.getAllChildren().size() == 4) {
				changeLatch.countDown();
			}
		};
		
		distributionRegionSyncer.registerCallback(callback);
		spacepartitionier.splitFailed(child1, destination);
		changeLatch.await();
		distributionRegionSyncer.unregisterCallback(callback);
		
		Assert.assertEquals(4, rootNode.getAllChildren().size());
	}
	
	@Test(timeout=60000)
	public void testMerge() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException {
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		
		Assert.assertEquals(0, rootNode.getDirectChildren().size());
		spacepartitionier.splitRegion(rootNode, new HashSet<>());
		Assert.assertEquals(4, rootNode.getDirectChildren().size());
		
		final DistributionRegion mergeRegion = spacepartitionier.getDestinationForMerge(
				rootNode.getDirectChildren());
		
		Assert.assertEquals(rootNode, mergeRegion);
		
		spacepartitionier.mergeFailed(rootNode.getDirectChildren(), mergeRegion);
	}
	
	/**
	 * Test region underflow and overflow
	 * @throws BBoxDBException
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testOverflowUnderflow() throws BBoxDBException, 
		ZookeeperException, ZookeeperNotFoundException, InterruptedException {
		
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		Assert.assertTrue(RegionSplitHelper.isSplittingSupported(rootNode));

		Assert.assertEquals(0, rootNode.getDirectChildren().size());
		Assert.assertTrue(RegionSplitHelper.isSplittingSupported(rootNode));
		Assert.assertFalse(RegionSplitHelper.isRegionOverflow(rootNode));
		
		final List<DistributionRegion> destination = spacepartitionier.splitRegion(rootNode, new HashSet<>());
		spacepartitionier.splitComplete(rootNode, destination);
		Assert.assertEquals(4, rootNode.getDirectChildren().size());
		
		Assert.assertEquals(1, RegionMergeHelper.getMergingCandidates(rootNode).size());
		Assert.assertEquals(4, RegionMergeHelper.getMergingCandidates(rootNode).get(0).size());

		Assert.assertTrue(RegionMergeHelper.isMergingBySpacePartitionerAllowed(rootNode));
		Assert.assertTrue(RegionMergeHelper.isMergingByZookeeperAllowed(rootNode));
		
		final CountDownLatch changeLatch = new CountDownLatch(1);
		
		final DistributionRegionCallback callback = (e, r) -> { 
			final boolean notAllReady = rootNode
					.getAllChildren()
					.stream()
					.anyMatch(a -> a.getState() != DistributionRegionState.ACTIVE);
			
			if(! notAllReady) {
				changeLatch.countDown();
			}
		};
		
		final DistributionRegionSyncer distributionRegionSyncer 
			= spacepartitionier.getDistributionRegionSyncer();
		
		distributionRegionSyncer.registerCallback(callback);
		changeLatch.await();
		distributionRegionSyncer.unregisterCallback(callback);
		
		final BBoxDBInstance system = rootNode.getSystems().get(0);
		Assert.assertFalse(RegionMergeHelper.isRegionUnderflow(destination, system));
		
		final BBoxDBInstance localName = ZookeeperClientFactory.getLocalInstanceName();
		Assert.assertFalse(RegionMergeHelper.isRegionUnderflow(destination, localName));
	}
	
	@Test(timeout=60000)
	public void testConfiguredRegionSize() throws ZookeeperException, 
		ZookeeperNotFoundException, BBoxDBException {
						
		final long regionSize = RegionMergeHelper.getConfiguredRegionMinSizeInMB(TEST_GROUP);
		Assert.assertEquals(Const.DEFAULT_MIN_REGION_SIZE, regionSize);
	}
	
	/**
	 * Test the restricted space
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 * @throws ZookeeperNotFoundException 
	 */
	@Test(timeout=60000)
	public void testRestrictedSpace() throws ZookeeperException, BBoxDBException, 
		ZookeeperNotFoundException {
		
		final Hyperrectangle completeSpace = new Hyperrectangle(0d, 10d, 0d, 10d);
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.QuadtreeSpacePartitioner", completeSpace.toCompactString())
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
		
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		Assert.assertEquals(completeSpace, rootNode.getConveringBox());
		
		spacepartitionier.splitRegion(rootNode, new HashSet<>());
		
		Assert.assertEquals("[[0.0,5.0):[0.0,5.0)]", rootNode.getChildNumber(0).getConveringBox().toCompactString());
		Assert.assertEquals("[[5.0,10.0]:[0.0,5.0)]", rootNode.getChildNumber(1).getConveringBox().toCompactString());
		Assert.assertEquals("[[0.0,5.0):[5.0,10.0]]", rootNode.getChildNumber(2).getConveringBox().toCompactString());
		Assert.assertEquals("[[5.0,10.0]:[5.0,10.0]]", rootNode.getChildNumber(3).getConveringBox().toCompactString());
	}
	
	/**
	 * Get the space partitioner
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private QuadtreeSpacePartitioner getSpacePartitioner() throws ZookeeperException, ZookeeperNotFoundException {
		final QuadtreeSpacePartitioner spacepartitionier = (QuadtreeSpacePartitioner) 
				distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP, 
						new HashSet<>(), new DistributionRegionIdMapper(TEST_GROUP));
				
		return spacepartitionier;
	}
	
}

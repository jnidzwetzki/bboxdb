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

import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.KDtreeSpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestKDtreeSpacePartitioner {
	
	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "abc";
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;	
	
	/**
	 * The compare delta
	 */
	private final static double DELTA = 0.0001;
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException {
		distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withReplicationFactor((short) 1)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}

	/**
	 * Test the root node dimensions
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws ZookeeperNotFoundException
	 * @throws BBoxDBException
	 */
	@Test(timeout=60000)
	public void testDimensionsOfRootNode() throws ZookeeperException, InterruptedException, ZookeeperNotFoundException, BBoxDBException {
		for(int i = 1; i < 2 ; i++) {
			createNewDistributionGroup(i); 
			final KDtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
			final DistributionRegion rootNode = spacepartitionier.getRootNode();
			Assert.assertEquals(i, rootNode.getConveringBox().getDimension());
		}
	}

	/**
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private KDtreeSpacePartitioner getSpacePartitioner() throws ZookeeperException, ZookeeperNotFoundException {
		return (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP, 
						new HashSet<>(), new DistributionRegionIdMapper(TEST_GROUP));		
	}

	/**
	 * Create a new distribution group
	 * 
	 * @param dimension
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 */
	private void createNewDistributionGroup(int dimension) throws ZookeeperException, BBoxDBException {
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(dimension)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration);
	}
	
	/**
	 * Test get merge candidates
	 * @throws BBoxDBException 
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ResourceAllocationException 
	 * @throws InterruptedException 
	 */
	@Test(timeout=60000)
	public void testGetMergeCandidates() throws ZookeeperException, BBoxDBException, 
		ZookeeperNotFoundException, ResourceAllocationException, InterruptedException {
		
		createNewDistributionGroup(2); 
		
		final KDtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		spacepartitionier.splitNode(rootNode, 10);
		spacepartitionier.waitForSplitCompleteZookeeperCallback(rootNode, 2);
		spacepartitionier.splitComplete(rootNode, rootNode.getDirectChildren());
		spacepartitionier.waitUntilNodeStateIs(rootNode, DistributionRegionState.SPLIT);
		Assert.assertEquals(rootNode.getState(), DistributionRegionState.SPLIT);
		
		final DistributionRegion childNumber0 = rootNode.getChildNumber(0);
		final DistributionRegion childNumber1 = rootNode.getChildNumber(1);

		Assert.assertTrue(spacepartitionier.getMergeCandidates(childNumber0).isEmpty());
		Assert.assertTrue(spacepartitionier.getMergeCandidates(childNumber1).isEmpty());
		
		spacepartitionier.splitNode(childNumber0, 10);
		spacepartitionier.waitForSplitCompleteZookeeperCallback(childNumber0, 2);
		spacepartitionier.splitComplete(childNumber0, childNumber0.getDirectChildren());
		spacepartitionier.waitUntilNodeStateIs(childNumber0, DistributionRegionState.SPLIT);
		Assert.assertEquals(childNumber0.getState(), DistributionRegionState.SPLIT);

		Assert.assertTrue(spacepartitionier.getMergeCandidates(childNumber0.getChildNumber(0)).isEmpty());
		Assert.assertTrue(spacepartitionier.getMergeCandidates(childNumber0.getChildNumber(1)).isEmpty());

		final List<List<DistributionRegion>> mergeCandidates0 = spacepartitionier.getMergeCandidates(childNumber0);

		Assert.assertFalse(mergeCandidates0.isEmpty());
		Assert.assertTrue(spacepartitionier.getMergeCandidates(childNumber1).isEmpty());
		
		for(final List<DistributionRegion> regions : mergeCandidates0) {
			Assert.assertTrue(regions.size() == 2);
		}
	}
	
	/**
	 * Test the split of a distribution region
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void testDistributionRegionSplitAndMerge() throws Exception {
		
		final DistributionRegionAdapter distributionRegionAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();	

		// Split and update
		System.out.println("---> Get space partitioner");
		final KDtreeSpacePartitioner spacePartitioner = (KDtreeSpacePartitioner) getSpacePartitioner();
		
		System.out.println("---> Get space partitioner - DONE");
		final DistributionRegion rootNode = spacePartitioner.getRootNode();
		System.out.println("---> Get root node - DONE");
		Assert.assertEquals(0, rootNode.getRegionId());
		
		Assert.assertEquals(TEST_GROUP, rootNode.getDistributionGroupName());
		
		final DistributionRegionState stateForDistributionRegion1 = distributionRegionAdapter.getStateForDistributionRegion(rootNode);
		Assert.assertEquals(DistributionRegionState.ACTIVE, stateForDistributionRegion1);
		System.out.println("--> Set split for node");
		spacePartitioner.splitNode(rootNode, 10);
	
		spacePartitioner.waitForSplitCompleteZookeeperCallback(rootNode, 2);

		final DistributionRegion firstChild = rootNode.getDirectChildren().get(0);
		Assert.assertEquals(10.0, firstChild.getConveringBox().getCoordinateHigh(0), DELTA);		
		final DistributionRegionState stateForDistributionRegion2 = distributionRegionAdapter.getStateForDistributionRegion(rootNode);
		Assert.assertEquals(DistributionRegionState.SPLITTING, stateForDistributionRegion2);

		// Reread group from zookeeper
		final DistributionRegion newDistributionGroup = spacePartitioner.getRootNode();
		final DistributionRegion firstChildNew = newDistributionGroup.getDirectChildren().get(0);
		Assert.assertEquals(10.0, firstChildNew.getConveringBox().getCoordinateHigh(0), DELTA);

		final DistributionRegionState stateForDistributionRegion3 = distributionRegionAdapter.getStateForDistributionRegion(newDistributionGroup);
		Assert.assertEquals(DistributionRegionState.SPLITTING, stateForDistributionRegion3);
		
		Assert.assertEquals(1, rootNode.getDirectChildren().get(0).getRegionId());
		Assert.assertEquals(2, rootNode.getDirectChildren().get(1).getRegionId());
		
		// Delete children
		System.out.println("---> Calling prepare merge");
		spacePartitioner.prepareMerge(spacePartitioner.getRootNode().getDirectChildren(), 
				spacePartitioner.getRootNode());
		
		System.out.println("---> Calling merge complete");
		spacePartitioner.mergeComplete(spacePartitioner.getRootNode().getDirectChildren(), 
				spacePartitioner.getRootNode());
		final DistributionRegion newDistributionGroup2 = spacePartitioner.getRootNode();
		final DistributionRegionState stateForDistributionRegion4 = distributionRegionAdapter.getStateForDistributionRegion(newDistributionGroup2);
		Assert.assertEquals(DistributionRegionState.ACTIVE, stateForDistributionRegion4);
	}
	
	/**
	 * Test the distribution of changes in the zookeeper structure (reading data from the second object)
	 */
	@Test(timeout=60000)
	public void testDistributionRegionSplitWithZookeeperPropergate() throws Exception {
		
		final KDtreeSpacePartitioner adapter1 = 
				(KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion distributionGroup1 = adapter1.getRootNode();
		
		final KDtreeSpacePartitioner adapter2 = 
				(KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion distributionGroup2 = adapter2.getRootNode();

		// Update object 1
		adapter1.splitNode(distributionGroup1, 10);

		// Sleep some seconds to wait for the update
		adapter2.waitForSplitCompleteZookeeperCallback(distributionGroup1, 2);

		// Read update from the second object
		final DistributionRegion firstChild = distributionGroup2.getDirectChildren().get(0);
		Assert.assertEquals(10.0, firstChild.getConveringBox().getCoordinateHigh(0), DELTA);
				
		// Check region ids
		Assert.assertEquals(0, distributionGroup2.getRegionId());
		Assert.assertEquals(1, distributionGroup2.getDirectChildren().get(0).getRegionId());
		Assert.assertEquals(2, distributionGroup2.getDirectChildren().get(1).getRegionId());
	}
	
	/**
	 * Test the distribution of changes in the zookeeper structure (reading data from the second object)
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 * @throws ZookeeperNotFoundException 
	 * @throws BBoxDBException 
	 * @throws ResourceAllocationException 
	 */
	@Test(timeout=60000)
	public void testDistributionRegionSplitWithZookeeperPropergate2() throws Exception {
		
		System.out.println("== Build KD adapter 1");
		final KDtreeSpacePartitioner adapter1 
			= (KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion distributionGroup1 = adapter1.getRootNode();
		Assert.assertEquals(2, distributionGroup1.getConveringBox().getDimension());
		
		System.out.println("== Build KD adapter 2");
		final KDtreeSpacePartitioner adapter2 
			= (KDtreeSpacePartitioner) getSpacePartitioner();
		final DistributionRegion distributionGroup2 = adapter2.getRootNode();
		Assert.assertEquals(2, distributionGroup2.getConveringBox().getDimension());

		Assert.assertEquals(0, distributionGroup1.getLevel());
		
		// Update object 1
		adapter1.splitNode(distributionGroup1, 10);
		
		final DistributionRegion leftChild = distributionGroup1.getDirectChildren().get(0);
		Assert.assertEquals(1, leftChild.getLevel());
		Assert.assertEquals(1, adapter2.getSplitDimension(leftChild));
		
		adapter1.splitNode(leftChild, 50);

		// Sleep 2 seconds to wait for the update
		adapter2.waitForSplitCompleteZookeeperCallback(leftChild, 2);

		// Read update from the second object
		final DistributionRegion firstChild = distributionGroup2.getDirectChildren().get(0);
		Assert.assertEquals(10.0, firstChild.getConveringBox().getCoordinateHigh(0), DELTA);
		
		final DistributionRegion secondChild = firstChild.getDirectChildren().get(0);
		Assert.assertEquals(50.0, secondChild.getConveringBox().getCoordinateHigh(1), DELTA);
	}
	
	/**
	 * Test the distribution region level
	 * @throws Exception  
	 */
	@Test(timeout=60000)
	public void testDistributionRegionLevel() throws Exception {
		
		final KDtreeSpacePartitioner kdTreeAdapter = (KDtreeSpacePartitioner) SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(TEST_GROUP);
		
		final DistributionRegion rootNode = kdTreeAdapter.getRootNode();
		
		Assert.assertEquals(1, rootNode.getTotalLevel());
		Assert.assertEquals(0, rootNode.getLevel());

		kdTreeAdapter.splitNode(rootNode, (float) 20.0);
		
		kdTreeAdapter.waitForSplitCompleteZookeeperCallback(rootNode, 2);
		
		Assert.assertEquals(2, rootNode.getTotalLevel());
		
		for(final DistributionRegion region : rootNode.getAllChildren()) {
			Assert.assertEquals(2, region.getTotalLevel());
			Assert.assertEquals(1, region.getLevel());
		}
	}
}

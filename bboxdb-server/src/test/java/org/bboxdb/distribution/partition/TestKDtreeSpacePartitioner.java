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
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
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
	
	@BeforeClass
	public static void before() {
		distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
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
						new HashSet<>(), new DistributionRegionIdMapper());		
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
	 */
	@Test(timeout=60000)
	public void testGetMergeCandidates() throws ZookeeperException, BBoxDBException, ZookeeperNotFoundException, ResourceAllocationException {
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
}

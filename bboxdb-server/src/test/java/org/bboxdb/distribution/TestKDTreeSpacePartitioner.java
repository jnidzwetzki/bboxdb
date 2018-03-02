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

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.KDtreeSpacePartitioner;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKDTreeSpacePartitioner {
	
	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "abc";
	
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	@BeforeClass
	public static void before() {
		distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	}

	@Test
	public void testRootNodeRefresh() throws ZookeeperException, InterruptedException {
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(2)); 
		
		final KDtreeSpacePartitioner spacepartitionier = (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
		final DistributionRegion oldRootNode = spacepartitionier.getRootNode();
		final DistributionRegionIdMapper mapper = spacepartitionier.getDistributionRegionIdMapper();
		
		Assert.assertEquals(0, mapper.getAllRegionIds().size());
		mapper.addMapping(3, BoundingBox.FULL_SPACE, TEST_GROUP);
		Assert.assertEquals(1, mapper.getAllRegionIds().size());
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(2)); 
		final DistributionRegion newRootNode = spacepartitionier.getRootNode();
		Assert.assertFalse(oldRootNode == newRootNode);
		Assert.assertEquals(0, mapper.getAllRegionIds().size());

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(2)); 
		Thread.sleep(1000);
		final DistributionRegion newRootNode2 = spacepartitionier.getRootNode();
		Assert.assertFalse(newRootNode2 == newRootNode);
	}
	
	@Test
	public void testDimensionsOfRootNode() throws ZookeeperException, InterruptedException {
		
		for(int i = 1; i < 2 ; i++) {
			distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
			distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, new DistributionGroupConfiguration(i)); 
			
			final KDtreeSpacePartitioner spacepartitionier = (KDtreeSpacePartitioner) distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP);
			final DistributionRegion rootNode = spacepartitionier.getRootNode();
			
			Assert.assertEquals(i, rootNode.getConveringBox().getDimension());
		}

	}
}

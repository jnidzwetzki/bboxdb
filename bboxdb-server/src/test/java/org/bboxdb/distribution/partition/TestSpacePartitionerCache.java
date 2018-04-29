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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.KDtreeSpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSpacePartitionerCache {
	
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

	@Test(timeout=60000)
	public void testRootNodeRefresh() throws Exception {
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
		
		final KDtreeSpacePartitioner oldSpacepartitionier = (KDtreeSpacePartitioner) 
				SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP);
		
		final DistributionRegion oldRootNode = oldSpacepartitionier.getRootNode();
		final DistributionRegionIdMapper mapper = oldSpacepartitionier.getDistributionRegionIdMapper();
		
		Assert.assertEquals(0, mapper.getAllRegionIds().size());
		mapper.addMapping(3, Hyperrectangle.FULL_SPACE);
		Assert.assertEquals(1, mapper.getAllRegionIds().size());
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
		
		final KDtreeSpacePartitioner newSpacepartitionier1 = (KDtreeSpacePartitioner) 
				SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP);
		
		final DistributionRegion newRootNode = newSpacepartitionier1.getRootNode();
		Assert.assertFalse(oldRootNode == newRootNode);
		Assert.assertEquals(0, mapper.getAllRegionIds().size());

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
		Thread.sleep(1000);
		
		final KDtreeSpacePartitioner newSpacepartitionier2 = (KDtreeSpacePartitioner) 
				SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP);
		
		final DistributionRegion newRootNode2 = newSpacepartitionier2.getRootNode();
		Assert.assertFalse(newRootNode2 == newRootNode);
	}

}

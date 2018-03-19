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

import org.bboxdb.distribution.partitioner.QuadtreeSpacePartitioner;
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

	@Test
	public void testSplit0() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, ResourceAllocationException {
		
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		
		Assert.assertEquals(0, rootNode.getDirectChildren().size());
		spacepartitionier.splitRegion(rootNode, new HashSet<>());
		Assert.assertEquals(4, rootNode.getDirectChildren().size());
	}
	
	@Test
	public void testSplit1() throws ZookeeperException, ZookeeperNotFoundException, BBoxDBException, ResourceAllocationException {
		
		final QuadtreeSpacePartitioner spacepartitionier = getSpacePartitioner();
		
		final DistributionRegion rootNode = spacepartitionier.getRootNode();
		
		Assert.assertEquals(0, rootNode.getDirectChildren().size());
		spacepartitionier.splitRegion(rootNode, new HashSet<>());
		Assert.assertEquals(4, rootNode.getDirectChildren().size());
		
		final DistributionRegion child1 = rootNode.getChildNumber(1);
		spacepartitionier.splitRegion(child1, new HashSet<>());
		Assert.assertEquals(4, child1.getDirectChildren().size());
		Assert.assertEquals(8, rootNode.getAllChildren().size());		
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
						new HashSet<>(), new DistributionRegionIdMapper());
				
		return spacepartitionier;
	}
	
}

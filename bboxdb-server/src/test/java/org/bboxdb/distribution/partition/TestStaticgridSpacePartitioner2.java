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

import org.bboxdb.distribution.partitioner.StaticgridSpacePartitioner;
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

public class TestStaticgridSpacePartitioner2 {
	
	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "staticgrid2";
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;
	
	@BeforeClass
	public static void beforeClass() throws ZookeeperException, BBoxDBException {
		distributionGroupZookeeperAdapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		
		final String config = "[[0.0,1.0]:[0.0,1.0]];0.5;0.5"; 
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.StaticgridSpacePartitioner", config)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}

	@Test(expected=BBoxDBException.class)
	public void testInvalidConfiguration1() throws ZookeeperException, BBoxDBException {
	final String config = "abc"; 
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.StaticgridSpacePartitioner", config)
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}
	
	@Test(expected=BBoxDBException.class)
	public void testInvalidConfiguration2() throws ZookeeperException, BBoxDBException {
		final String config = "[[0.0,5.0]:[0.0,6.0]];0.5"; 
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.StaticgridSpacePartitioner", config)
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}
	
	@Test(expected=BBoxDBException.class)
	public void testInvalidConfiguration3() throws ZookeeperException, BBoxDBException {
		final String config = "[[0.0,5.0]:[0.0,6.0]];0.5;0.5;0.5"; 
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.StaticgridSpacePartitioner", config)
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}
	
	@Test(expected=BBoxDBException.class)
	public void testInvalidConfiguration4() throws ZookeeperException, BBoxDBException {
		final String config = ""; 
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.StaticgridSpacePartitioner", config)
				.build();

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 
	}
	
	/**
	 * Get the space partitioner
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private StaticgridSpacePartitioner getSpacePartitioner() throws ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacepartitionier = (StaticgridSpacePartitioner) 
				distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP, 
						new HashSet<>(), new DistributionRegionIdMapper(TEST_GROUP));
				
		return spacepartitionier;
	}
	
	/**
	 * Test the split region call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testSplitRegion() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.splitRegion(null, null);
	}
	
	/**
	 * Test the split complete call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testSplitComplete() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.splitComplete(null, null);
	}
	
	/**
	 * Test the split failed call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testSplitFailed() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.splitFailed(null, null);
	}
	
	/**
	 * Test the merge complete call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testMergeComplete() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.mergeComplete(null, null);
	}
	
	/**
	 * Test the merge failed call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testMergeFailed() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.mergeFailed(null, null);
	}
	
	/**
	 * Test the get destination for merge call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testGetDestinationForMerge() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.getDestinationForMerge(null);
	}
	
	/**
	 * Test the prepare merge call
	 * @throws BBoxDBException 
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(expected=BBoxDBException.class)
	public void testPrepareMerge() throws BBoxDBException, ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		spacePartitioner.prepareMerge(null, null);
	}
	
	/**
	 * Test the is splittable call
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	@Test(timeout=60000)
	public void testIsSplittable() throws ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		Assert.assertFalse(spacePartitioner.isSplitable(null));
	}
	
	/**
	 * Get the merge candidates
	 */
	@Test(timeout=60000)
	public void testGetMergeCandidates() throws ZookeeperException, ZookeeperNotFoundException {
		final StaticgridSpacePartitioner spacePartitioner = getSpacePartitioner();
		Assert.assertTrue(spacePartitioner.getMergeCandidates(null).isEmpty());
	}
}

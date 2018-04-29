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

import java.util.Collection;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingBasedSplitStrategy;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingHelper;
import org.bboxdb.distribution.partitioner.regionsplit.SimpleSplitStrategy;
import org.bboxdb.distribution.partitioner.regionsplit.SplitpointStrategy;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSampling {
	
	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;

	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "abc";
	
	/**
	 * The storage registry
	 */
	private static TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The name of the test relation
	 */
	private final static TupleStoreName TEST_RELATION = new TupleStoreName(TEST_GROUP + "_relation3_0");
	

	@BeforeClass
	public static void beforeClass() throws InterruptedException, BBoxDBException {
		distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
	}
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException, StorageManagerException {
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		storageRegistry.deleteAllTablesInDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 		
	}

	/**
	 * Test the sampling
	 * @throws RejectedException 
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testSampling1() throws StorageManagerException, RejectedException, BBoxDBException {
				
		createDummyTable();
		
		final DistributionRegion rootNode 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP).getRootNode();
		
		final Collection<Hyperrectangle> samples 
			= SamplingHelper.getSamplesForRegion(rootNode, storageRegistry);
		
		Assert.assertFalse(samples.isEmpty());		
	}

	/**
	 * Create a dummy table
	 * @throws StorageManagerException
	 * @throws RejectedException
	 */
	private void createDummyTable() throws StorageManagerException, RejectedException {
		
		final TupleStoreManager table 
			= storageRegistry.createTable(TEST_RELATION, new TupleStoreConfiguration());
		
		for(int i = 0; i < 100; i++) {
			table.put(new Tuple(Integer.toString(i), new Hyperrectangle(1d, 2d, 1d, 20d), "".getBytes()));
			table.put(new DeletedTuple(Integer.toString(i+10000)));
		}
	}
	
	/**
	 * Test the sampling (without tuples)
	 * @throws RejectedException 
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testSampling2() throws StorageManagerException, RejectedException, BBoxDBException {
				
		storageRegistry.createTable(TEST_RELATION, new TupleStoreConfiguration());
		
		final DistributionRegion rootNode 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP).getRootNode();
		
		final Collection<Hyperrectangle> samples 
			= SamplingHelper.getSamplesForRegion(rootNode, storageRegistry);
		
		Assert.assertTrue(samples.isEmpty());		
	}
	
	/**
	 * Test the sampling
	 * @throws RejectedException 
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	@Test(timeout=60000)
	public void testSamplingBasedSplitStrategy1() throws StorageManagerException, RejectedException, BBoxDBException {
				
		createDummyTable();
		
		final DistributionRegion rootNode 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP).getRootNode();
		
		final Collection<Hyperrectangle> samples 
			= SamplingHelper.getSamplesForRegion(rootNode, storageRegistry);
		
		Assert.assertFalse(samples.isEmpty());		
		
		final SplitpointStrategy splitpointStrategy = new SamplingBasedSplitStrategy(samples);

		final Hyperrectangle coveringBox1 = new Hyperrectangle(1d, 2d, -1d, 20d);
		final double splitPoint0 = splitpointStrategy.getSplitPoint(0, coveringBox1);
		final double splitPoint1 = splitpointStrategy.getSplitPoint(1, coveringBox1);
		Assert.assertTrue(coveringBox1.isCoveringPointInDimension(splitPoint0, 0));
		Assert.assertTrue(coveringBox1.isCoveringPointInDimension(splitPoint1, 1));	
		
		final Hyperrectangle coveringBox2 = new Hyperrectangle(1.5d, 2d, -1d, 20d);
		final double splitPoint02 = splitpointStrategy.getSplitPoint(0, coveringBox2);
		final double splitPoint12 = splitpointStrategy.getSplitPoint(1, coveringBox2);
		Assert.assertTrue(coveringBox1.isCoveringPointInDimension(splitPoint02, 0));
		Assert.assertTrue(coveringBox1.isCoveringPointInDimension(splitPoint12, 1));	
		
		final Hyperrectangle coveringBox3 = new Hyperrectangle(-1.5d, 2d, -1d, 20d);
		final double splitPoint03 = splitpointStrategy.getSplitPoint(0, coveringBox3);
		final double splitPoint13 = splitpointStrategy.getSplitPoint(1, coveringBox3);
		Assert.assertTrue(coveringBox1.isCoveringPointInDimension(splitPoint03, 0));
		Assert.assertTrue(coveringBox1.isCoveringPointInDimension(splitPoint13, 1));	
	}
	
	/**
	 * Test the sampling
	 * @throws RejectedException 
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	@Test(expected=StorageManagerException.class)
	public void testSamplingBasedSplitStrategy2() throws StorageManagerException, RejectedException, BBoxDBException {
				
		createDummyTable();
		
		final DistributionRegion rootNode 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP).getRootNode();
		
		final Collection<Hyperrectangle> samples 
			= SamplingHelper.getSamplesForRegion(rootNode, storageRegistry);
		
		Assert.assertFalse(samples.isEmpty());		
		
		final SplitpointStrategy splitpointStrategy = new SamplingBasedSplitStrategy(samples);

		final Hyperrectangle coveringBox = new Hyperrectangle(10d, 20d, -1d, 20d);
		
		final double splitPoint0 = splitpointStrategy.getSplitPoint(0, coveringBox);
		final double splitPoint1 = splitpointStrategy.getSplitPoint(1, coveringBox);
		
		System.out.println("Split point0: " + splitPoint0);
		System.out.println("Split point1: " + splitPoint1);
		
		Assert.assertTrue(coveringBox.isCoveringPointInDimension(splitPoint0, 0));
		Assert.assertTrue(coveringBox.isCoveringPointInDimension(splitPoint1, 1));	
	}
	
	/**
	 * Test the simple splitpoint strategy
	 * @throws StorageManagerException 
	 */
	@Test(timeout=60000)
	public void testSimpleSplitpointStrategy1() throws StorageManagerException {
		final SplitpointStrategy splitpointStrategy = new SimpleSplitStrategy();
		final Hyperrectangle coveringBox = new Hyperrectangle(1d, 2d, -1d, 20d);
		
		final double splitPoint0 = splitpointStrategy.getSplitPoint(0, coveringBox);
		final double splitPoint1 = splitpointStrategy.getSplitPoint(1, coveringBox);
		
		Assert.assertTrue(coveringBox.isCoveringPointInDimension(splitPoint0, 0));
		Assert.assertTrue(coveringBox.isCoveringPointInDimension(splitPoint1, 1));
	}
}

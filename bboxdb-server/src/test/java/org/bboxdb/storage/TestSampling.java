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
package org.bboxdb.storage;

import java.util.Collection;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingHelper;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
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
	private static DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;

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
		distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
		storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();
	}
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException, StorageManagerException {
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		storageRegistry.deleteAllTablesInDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration); 		
	}

	/**
	 * Test the sampling
	 * @throws RejectedException 
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testSampling1() throws StorageManagerException, RejectedException, BBoxDBException {
				
		final TupleStoreManager table 
			= storageRegistry.createTable(TEST_RELATION, new TupleStoreConfiguration());
		
		for(int i = 0; i < 100; i++) {
			table.put(new Tuple(Integer.toString(i), new BoundingBox(1d, 2d, 1d, 2d), "".getBytes()));
		}
		
		final DistributionRegion rootNode 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP).getRootNode();
		
		final Collection<BoundingBox> samples 
			= SamplingHelper.getSamplesForRegion(rootNode, storageRegistry);
		
		Assert.assertFalse(samples.isEmpty());		
	}
	
	/**
	 * Test the sampling (without tuples)
	 * @throws RejectedException 
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	@Test
	public void testSampling2() throws StorageManagerException, RejectedException, BBoxDBException {
				
		storageRegistry.createTable(TEST_RELATION, new TupleStoreConfiguration());
		
		final DistributionRegion rootNode 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(TEST_GROUP).getRootNode();
		
		final Collection<BoundingBox> samples 
			= SamplingHelper.getSamplesForRegion(rootNode, storageRegistry);
		
		Assert.assertTrue(samples.isEmpty());		
	}
}

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
package org.bboxdb.test.distribution.partition;

import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

import org.bboxdb.distribution.partitioner.DynamicgridSpacePartitioner;
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
import org.bboxdb.storage.util.EnvironmentHelper;
import org.bboxdb.test.BBoxDBTestHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHelper {
	/**
	 * The name of the test region
	 */
	private static final String TEST_GROUP = "helpergroup";

	/**
	 * The distribution group adapter
	 */
	private static DistributionGroupAdapter distributionGroupZookeeperAdapter;

	@BeforeClass
	public static void beforeClass() throws ZookeeperException {
		EnvironmentHelper.resetTestEnvironment();

		distributionGroupZookeeperAdapter
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
	}
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException {

		final String config = "[[0.0,5.0]:[0.0,6.0]];0.5";

		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withReplicationFactor((short) 1)
				.withSpacePartitioner("org.bboxdb.distribution.partitioner.DynamicgridSpacePartitioner", config)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		// Add fake instances for testing
		BBoxDBTestHelper.registerFakeInstance(2);

		distributionGroupZookeeperAdapter.deleteDistributionGroup(TEST_GROUP);
		distributionGroupZookeeperAdapter.createDistributionGroup(TEST_GROUP, configuration);
	}
	
	@Test(timeout=60_000)
	public void testNodeDeletion1() throws Exception {
		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();

		final DistributionRegion rootElement = spacePartitioner.getRootNode();
		final DistributionRegion elementToRemove = rootElement.getChildNumber(0).getChildNumber(0);

		final CountDownLatch latch = new CountDownLatch(1);
		
		final Thread thread = new Thread(() -> {
			try {
				latch.countDown();
				spacePartitioner.waitUntilNodeIsRemoved(elementToRemove);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}); 
		
		// Wait until thread is started and waits for the removal
		thread.start();
		latch.await();
		Thread.sleep(1000);
		
		final DistributionRegionAdapter distributionRegionZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
	
		distributionRegionZookeeperAdapter.deleteChild(elementToRemove);
		
		// Wait for removal
		thread.join(100000000);
		
		// Wait for a already removed node
		spacePartitioner.waitUntilNodeIsRemoved(elementToRemove);
	}
	
	@Test(timeout=60_000)
	public void testNodeDeletion2() throws Exception {
		final DynamicgridSpacePartitioner spacePartitioner = getSpacePartitioner();

		final DistributionRegion rootElement = spacePartitioner.getRootNode();
		final DistributionRegion elementToRemove = rootElement.getChildNumber(0).getChildNumber(0);

		final DistributionRegionAdapter distributionRegionZookeeperAdapter 
		= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();

		
		distributionRegionZookeeperAdapter.deleteChild(elementToRemove);

		spacePartitioner.waitUntilNodeIsRemoved(elementToRemove);
		
		// Wait for a already removed node
		spacePartitioner.waitUntilNodeIsRemoved(elementToRemove);
	}
	
	/**
	 * Get the space partitioner
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private DynamicgridSpacePartitioner getSpacePartitioner() throws ZookeeperException, ZookeeperNotFoundException {
		final DynamicgridSpacePartitioner spacepartitionier = (DynamicgridSpacePartitioner)
				distributionGroupZookeeperAdapter.getSpaceparitioner(TEST_GROUP,
						new HashSet<>(), new DistributionRegionIdMapper(TEST_GROUP));

		return spacepartitionier;
	}
}

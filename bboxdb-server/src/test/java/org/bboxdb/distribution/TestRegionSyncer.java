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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionChangedCallback;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestRegionSyncer {
	
	/**
	 * The test group
	 */
	private final static DistributionGroupName GROUP = new DistributionGroupName("synctestgroup");
	
	/**
	 * The adapter
	 */
	private final DistributionGroupZookeeperAdapter distributionGroupAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
	
	@Before
	public void before() throws ZookeeperException {
		distributionGroupAdapter.deleteDistributionGroup(GROUP.getFullname());
		distributionGroupAdapter.createDistributionGroup(GROUP.getFullname(), 
				new DistributionGroupConfiguration(2));
	}
	
	@Test
	public void getNonExistingRoot() throws ZookeeperException {
		distributionGroupAdapter.deleteDistributionGroup(GROUP.getFullname());
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		Assert.assertEquals(null, root);
	}
	
	@Test
	public void getExistingRoot() throws ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		Assert.assertTrue(root != null);
	}
	
	@Test(timeout=10000)
	public void waitForSystemsCallback1() throws ZookeeperException, InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		final DistributionRegionChangedCallback callback = (r) -> { if(r.equals(root)) { latch.countDown(); }};
		distributionRegionSyncer.registerCallback(callback);
		
		final BBoxDBInstance newInstance = new BBoxDBInstance("localhost:8443");
		distributionGroupAdapter.addSystemToDistributionRegion(root, newInstance);
		
		latch.await();
		distributionRegionSyncer.unregisterCallback(callback);

		Assert.assertTrue(root.getSystems().contains(newInstance));
	}
	
	@Test(timeout=10000)
	public void testSplit1() throws ZookeeperException, InterruptedException {
		
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		final String fullname = createSplittedRoot(distributionRegionSyncer, root);
		
		System.out.println("==== Creating subchild");
		
		final DistributionRegion level1Child = root.getChildNumber(0);
		final BoundingBox leftBoundingBoxChild = level1Child.getConveringBox().splitAndGetLeft(0, 1, true);
		
		final CountDownLatch latchLevel1 = new CountDownLatch(1);
		final String level1ChildPath = distributionGroupAdapter.getZookeeperPathForDistributionRegion(level1Child);

		final DistributionRegionChangedCallback level1Callback = (r) -> { if(root.getAllChildren().size() == 3) { latchLevel1.countDown(); }};
		
		distributionRegionSyncer.registerCallback(level1Callback);
		distributionGroupAdapter.createNewChild(level1ChildPath, 0, leftBoundingBoxChild, fullname);
		latchLevel1.await();
		distributionRegionSyncer.unregisterCallback(level1Callback);
		
		Assert.assertTrue(root.getChildNumber(0).getChildNumber(0) != null);
		Assert.assertEquals(3, root.getAllChildren().size());
		
		Assert.assertEquals(0, root.getLevel());
		Assert.assertEquals(1, level1Child.getLevel());
		Assert.assertEquals(2, root.getChildNumber(0).getChildNumber(0).getLevel());
	}

	private String createSplittedRoot(final DistributionRegionSyncer distributionRegionSyncer,
			final DistributionRegion root) throws ZookeeperException, InterruptedException {
		
		final BoundingBox leftBoundingBox = root.getConveringBox().splitAndGetLeft(0, 0, true);
		final BoundingBox rightBoundingBox = root.getConveringBox().splitAndGetRight(0, 0, true);
		
		final String regionPath = distributionGroupAdapter.getZookeeperPathForDistributionRegion(root);

		final String fullname = GROUP.getFullname();
		
		final CountDownLatch latchLevel0 = new CountDownLatch(1);
		final DistributionRegionChangedCallback level0Callback = (r) -> { if(root.getDirectChildren().size() == 2) { latchLevel0.countDown(); }};

		distributionRegionSyncer.registerCallback(level0Callback);
		distributionGroupAdapter.createNewChild(regionPath, 0, leftBoundingBox, fullname);
		distributionGroupAdapter.createNewChild(regionPath, 1, rightBoundingBox, fullname);
		latchLevel0.await();
		distributionRegionSyncer.unregisterCallback(level0Callback);
		
		Assert.assertEquals(2, root.getDirectChildren().size());
		Assert.assertTrue(root.getChildNumber(0) != null);
		Assert.assertTrue(root.getChildNumber(1) != null);
		return fullname;
	}

	@Test(timeout=10000)
	public void testChangeState() throws InterruptedException, ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		Assert.assertEquals(DistributionRegionState.ACTIVE, root.getState());
		
		final CountDownLatch latch = new CountDownLatch(1);
		final DistributionRegionChangedCallback callback = (r) -> { if(r.getState() == DistributionRegionState.ACTIVE_FULL) { latch.countDown(); }};
		distributionRegionSyncer.registerCallback(callback);
		
		distributionGroupAdapter.setStateForDistributionRegion(root, DistributionRegionState.ACTIVE_FULL);
		
		latch.await();
		distributionRegionSyncer.unregisterCallback(callback);
		
		Assert.assertEquals(DistributionRegionState.ACTIVE_FULL, root.getState());
	}
	
	@Test(timeout=10000)
	public void testMerge() throws ZookeeperException, InterruptedException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		createSplittedRoot(distributionRegionSyncer, root);
		
		// Delete child 1
		System.out.println("=== Delete child 1");
		final CountDownLatch deleteLatch1 = new CountDownLatch(1);
		
		final DistributionRegionChangedCallback callback1 = (r) -> { if(r.getAllChildren().size() == 1) { deleteLatch1.countDown(); }};
		distributionRegionSyncer.registerCallback(callback1);
		distributionGroupAdapter.deleteChild(root.getChildNumber(1));
		deleteLatch1.await();
		distributionRegionSyncer.unregisterCallback(callback1);

		Assert.assertEquals(1, root.getAllChildren().size());
		
		// Delete child 2
		System.out.println("=== Delete child 2");
		final CountDownLatch deleteLatch2 = new CountDownLatch(1);
		
		final DistributionRegionChangedCallback callback2 = (r) -> { if(r.getAllChildren().size() == 0) { deleteLatch2.countDown(); }};
		distributionRegionSyncer.registerCallback(callback2);
		distributionGroupAdapter.deleteChild(root.getChildNumber(0));
		deleteLatch2.await();
		distributionRegionSyncer.unregisterCallback(callback2);

		Assert.assertEquals(0, root.getAllChildren().size());
	}
	
	/**
	 * Build a new syncer
	 */
	private DistributionRegionSyncer buildSyncer() {
		final Set<DistributionRegionChangedCallback> callbacks = new HashSet<>();
		final DistributionRegionIdMapper distributionRegionIdMapper = new DistributionRegionIdMapper();
		
		return new DistributionRegionSyncer(GROUP, distributionGroupAdapter, 
				distributionRegionIdMapper, callbacks);
	}
}

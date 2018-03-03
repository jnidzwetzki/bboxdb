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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
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
	public void waitForSystemsCallback() throws ZookeeperException, InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		final DistributionRegionCallback callback = (e, r) -> { if(r.equals(root)) { latch.countDown(); }};
		distributionRegionSyncer.registerCallback(callback);
		
		final BBoxDBInstance newInstance = new BBoxDBInstance("localhost:8443");
		distributionGroupAdapter.addSystemToDistributionRegion(root, newInstance);
		
		latch.await();
		distributionRegionSyncer.unregisterCallback(callback);

		Assert.assertTrue(root.getSystems().contains(newInstance));
	}
	
	@Test(timeout=10000)
	public void testSplit() throws ZookeeperException, InterruptedException {
		
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		final String fullname = createSplittedRoot(distributionRegionSyncer, root);
		
		System.out.println("==== Creating subchild");
		
		final DistributionRegion level1Child = root.getChildNumber(0);
		final BoundingBox leftBoundingBoxChild = level1Child.getConveringBox().splitAndGetLeft(0, 1, true);
		
		final CountDownLatch latchLevel1 = new CountDownLatch(1);
		final String level1ChildPath = distributionGroupAdapter.getZookeeperPathForDistributionRegion(level1Child);

		final DistributionRegionCallback level1Callback = (e, r) -> { if(root.getAllChildren().size() == 3) { latchLevel1.countDown(); }};
		
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

	@Test(timeout=10000)
	public void testChangeState() throws InterruptedException, ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		Assert.assertEquals(DistributionRegionState.ACTIVE, root.getState());
		
		final CountDownLatch latch = new CountDownLatch(1);
		final DistributionRegionCallback callback = (e, r) -> { if(r.getState() == DistributionRegionState.ACTIVE_FULL) { latch.countDown(); }};
		distributionRegionSyncer.registerCallback(callback);
		
		distributionGroupAdapter.setStateForDistributionRegion(root, DistributionRegionState.ACTIVE_FULL);
		
		latch.await();
		distributionRegionSyncer.unregisterCallback(callback);
		
		Assert.assertEquals(DistributionRegionState.ACTIVE_FULL, root.getState());
	}
	
	@Test(timeout=10000)
	public void testEventTypes() throws InterruptedException, ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		// Root node is changed on add and remove children
		final CountDownLatch changedLatch = new CountDownLatch(2);
		
		final DistributionRegionCallback rootNodeChangedCallback = (e, r) -> { 
			if(e == DistributionRegionEvent.CHANGED && r.equals(root)) { 
				changedLatch.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(rootNodeChangedCallback);

		// Two new children will be added
		final CountDownLatch addedLatch = new CountDownLatch(2);

		final DistributionRegionCallback adddedCallback = (e, r) -> { 
			if(e == DistributionRegionEvent.ADDED && ! r.equals(root)) { 
				addedLatch.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(adddedCallback);

		// Two children will be removed
		final CountDownLatch removedLatch = new CountDownLatch(2);

		final DistributionRegionCallback removedCallback = (e, r) -> { 
			if(e == DistributionRegionEvent.REMOVED && ! r.equals(root)) { 
				removedLatch.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(removedCallback);
		
		// Mutate tree
		createSplittedRoot(distributionRegionSyncer, root);

		// Wait
		System.out.println("=== Wait for added latch");
		addedLatch.await();
		
		distributionGroupAdapter.deleteChild(root.getChildNumber(0));
		distributionGroupAdapter.deleteChild(root.getChildNumber(1));

		System.out.println("=== Wait for removed latch");
		removedLatch.await();
		
		System.out.println("=== Wait for changed latch");
		changedLatch.await();
		
		distributionRegionSyncer.unregisterCallback(rootNodeChangedCallback);
		distributionRegionSyncer.unregisterCallback(adddedCallback);
		distributionRegionSyncer.unregisterCallback(removedCallback);
	}
	
	@Test(timeout=10000)
	public void testEventOnDelete() throws InterruptedException, ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		createSplittedRoot(distributionRegionSyncer, root);

		// Tree children will be removed
		final CountDownLatch removedLatch = new CountDownLatch(3);
		
		final DistributionRegionCallback removedCallback = (e, r) -> { 
			if(e == DistributionRegionEvent.REMOVED) { 
				removedLatch.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(removedCallback);

		distributionGroupAdapter.deleteDistributionGroup(GROUP.getFullname());

		System.out.println("=== testEventOnDelete: Wait for removed latch");
		removedLatch.await();
		
		distributionRegionSyncer.unregisterCallback(removedCallback);
	}
	
	@Test(timeout=10000)
	public void testMerge() throws ZookeeperException, InterruptedException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		createSplittedRoot(distributionRegionSyncer, root);
		
		// Delete child 1
		System.out.println("=== Delete child 1");
		final CountDownLatch deleteLatch1 = new CountDownLatch(1);
		
		final DistributionRegionCallback callback1 = (e, r) -> { 
			if(root.getAllChildren().size() == 1) { 
				deleteLatch1.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(callback1);
		distributionGroupAdapter.deleteChild(root.getChildNumber(1));
		deleteLatch1.await();
		distributionRegionSyncer.unregisterCallback(callback1);

		Assert.assertEquals(1, root.getAllChildren().size());
		
		// Delete child 2
		System.out.println("=== Delete child 2");
		final CountDownLatch deleteLatch2 = new CountDownLatch(1);
		
		final DistributionRegionCallback callback2 = (e, r) -> { 
			if(root.getAllChildren().size() == 0) { 
				deleteLatch2.countDown(); 
			}
		};
		
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
		final Set<DistributionRegionCallback> callbacks = new CopyOnWriteArraySet<>();
		final DistributionRegionIdMapper distributionRegionIdMapper = new DistributionRegionIdMapper();
		
		return new DistributionRegionSyncer(GROUP, distributionGroupAdapter, 
				distributionRegionIdMapper, callbacks);
	}
	
	/**
	 * Create a splitted root
	 * @param distributionRegionSyncer
	 * @param root
	 * @return
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	private String createSplittedRoot(final DistributionRegionSyncer distributionRegionSyncer,
			final DistributionRegion root) throws ZookeeperException, InterruptedException {
		
		final BoundingBox leftBoundingBox = root.getConveringBox().splitAndGetLeft(0, 0, true);
		final BoundingBox rightBoundingBox = root.getConveringBox().splitAndGetRight(0, 0, true);
		
		final String regionPath = distributionGroupAdapter.getZookeeperPathForDistributionRegion(root);

		final String fullname = GROUP.getFullname();
		
		final CountDownLatch latchLevel0 = new CountDownLatch(1);
		final DistributionRegionCallback level0Callback = (e, r) -> { if(root.getDirectChildren().size() == 2) { latchLevel0.countDown(); }};

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
}

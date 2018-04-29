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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitionerContext;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.region.DistributionRegionSyncer;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestRegionSyncer {
	
	/**
	 * The test group
	 */
	private final static String GROUP = "synctestgroup";
	
	/**
	 * The group adapter
	 */
	private final DistributionGroupAdapter distributionGroupAdapter = 
			ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
	
	/**
	 * The region adapter
	 */
	private final DistributionRegionAdapter distributionRegionAdapter = 
			ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
	
	@Before
	public void before() throws ZookeeperException, BBoxDBException {
		
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
				.create(2)
				.withPlacementStrategy("org.bboxdb.distribution.placement.DummyResourcePlacementStrategy", "")
				.build();
		
		distributionGroupAdapter.deleteDistributionGroup(GROUP);
		distributionGroupAdapter.createDistributionGroup(GROUP, configuration);
	}
	
	@Test(timeout=60000)
	public void getNonExistingRoot() throws ZookeeperException {
		distributionGroupAdapter.deleteDistributionGroup(GROUP);
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		Assert.assertEquals(null, root);
	}
	
	@Test(timeout=60000)
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
		Assert.assertEquals(0, root.getHighestChildNumber());

		final DistributionRegionCallback callback = (e, r) -> { if(r.equals(root)) { latch.countDown(); }};
		distributionRegionSyncer.registerCallback(callback);
		
		final BBoxDBInstance newInstance = new BBoxDBInstance("localhost:8443");
		final String path = distributionRegionAdapter.getZookeeperPathForDistributionRegion(root);
		distributionRegionAdapter.addSystemToDistributionRegion(path, newInstance);
		
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
		final Hyperrectangle leftBoundingBoxChild = level1Child.getConveringBox().splitAndGetLeft(0, 1, true);
		
		final CountDownLatch latchLevel1 = new CountDownLatch(1);
		final String level1ChildPath = distributionRegionAdapter.getZookeeperPathForDistributionRegion(level1Child);

		final DistributionRegionCallback level1Callback = (e, r) -> { if(root.getAllChildren().size() == 3) { latchLevel1.countDown(); }};
		
		distributionRegionSyncer.registerCallback(level1Callback);
		distributionRegionAdapter.createNewChild(level1ChildPath, 0, leftBoundingBoxChild, fullname);
		latchLevel1.await();
		distributionRegionSyncer.unregisterCallback(level1Callback);
		
		Assert.assertTrue(root.getChildNumber(0).getChildNumber(0) != null);
		Assert.assertEquals(0, root.getRegionId());
		Assert.assertEquals(1, root.getChildNumber(0).getRegionId());
		Assert.assertEquals(3, root.getChildNumber(0).getChildNumber(0).getRegionId());
		Assert.assertEquals(3, root.getAllChildren().size());
		
		Assert.assertEquals(0, root.getLevel());
		Assert.assertEquals(1, level1Child.getLevel());
		Assert.assertEquals(2, root.getChildNumber(0).getChildNumber(0).getLevel());
		
		// Reread data from zookeeper
		System.out.println("== clear in memory data");
		distributionRegionSyncer.clear();
		final DistributionRegion root2 = distributionRegionSyncer.getRootNode();
		
		Assert.assertTrue(root2.getChildNumber(0).getChildNumber(0) != null);
		Assert.assertEquals(0, root2.getRegionId());
		Assert.assertEquals(1, root2.getChildNumber(0).getRegionId());
		Assert.assertEquals(3, root2.getChildNumber(0).getChildNumber(0).getRegionId());
		Assert.assertEquals(3, root2.getAllChildren().size());
		
		Assert.assertEquals(0, root2.getLevel());
		Assert.assertEquals(2, root2.getChildNumber(0).getChildNumber(0).getLevel());
	}

	@Test(timeout=10000)
	public void testChangeState1() throws InterruptedException, ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		Assert.assertEquals(DistributionRegionState.ACTIVE, root.getState());
		
		final CountDownLatch latch = new CountDownLatch(1);
		final DistributionRegionCallback callback = (e, r) -> { if(r.getState() == DistributionRegionState.ACTIVE_FULL) { latch.countDown(); }};
		distributionRegionSyncer.registerCallback(callback);
		
		distributionRegionAdapter.setStateForDistributionRegion(root, DistributionRegionState.ACTIVE_FULL);
		
		latch.await();
		distributionRegionSyncer.unregisterCallback(callback);
		
		Assert.assertEquals(DistributionRegionState.ACTIVE_FULL, root.getState());
	}
	
	@Test(timeout=10000)
	public void testChangeState2() throws InterruptedException, ZookeeperException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		createSplittedRoot(distributionRegionSyncer, root);
				
		final CountDownLatch latch = new CountDownLatch(1);
		final DistributionRegionCallback callback = (e, r) -> { 
			if(r == root.getChildNumber(1) && r.getState() == DistributionRegionState.MERGING) { 
				latch.countDown();
			}
		};
		
		distributionRegionSyncer.registerCallback(callback);
		
		distributionRegionAdapter.setStateForDistributionRegion(root.getChildNumber(0), DistributionRegionState.MERGING);
		distributionRegionAdapter.setStateForDistributionRegion(root.getChildNumber(1), DistributionRegionState.MERGING);

		latch.await();
		distributionRegionSyncer.unregisterCallback(callback);
		
		Assert.assertEquals(2, root.getDirectChildren().size());
		Assert.assertEquals(DistributionRegionState.MERGING, root.getChildNumber(0).getState());
		Assert.assertEquals(DistributionRegionState.MERGING, root.getChildNumber(1).getState());
		
		// Reread state from zookeeper
		System.out.println("== clear in memory data");
		distributionRegionSyncer.clear();
		final DistributionRegion root2 = distributionRegionSyncer.getRootNode();

		Assert.assertEquals(2, root2.getDirectChildren().size());
		Assert.assertEquals(DistributionRegionState.MERGING, root2.getChildNumber(0).getState());
		Assert.assertEquals(DistributionRegionState.MERGING, root2.getChildNumber(1).getState());
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
		
		distributionRegionAdapter.deleteChild(root.getChildNumber(0));
		distributionRegionAdapter.deleteChild(root.getChildNumber(1));

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

		distributionGroupAdapter.deleteDistributionGroup(GROUP);

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
		distributionRegionAdapter.deleteChild(root.getChildNumber(1));
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
		distributionRegionAdapter.deleteChild(root.getChildNumber(0));
		deleteLatch2.await();
		distributionRegionSyncer.unregisterCallback(callback2);

		Assert.assertEquals(0, root.getAllChildren().size());
	}
	
	@Test(timeout=10000)
	public void testDeleteAndAdd() throws ZookeeperException, InterruptedException {
		final DistributionRegionSyncer distributionRegionSyncer = buildSyncer();
		final DistributionRegion root = distributionRegionSyncer.getRootNode();
		
		createSplittedRoot(distributionRegionSyncer, root);
		Assert.assertEquals(2, root.getAllChildren().size());
		
		// Delete child 1
		System.out.println("=== Delete child 1");
		final CountDownLatch deleteLatch1 = new CountDownLatch(1);
		
		final DistributionRegionCallback callback1 = (e, r) -> { 
			if(root.getAllChildren().size() == 1) { 
				deleteLatch1.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(callback1);
		distributionRegionAdapter.deleteChild(root.getChildNumber(0));
		deleteLatch1.await();
		distributionRegionSyncer.unregisterCallback(callback1);

		Assert.assertEquals(1, root.getAllChildren().size());
		
		// Add new child
		System.out.println("=== Add new child");
		final CountDownLatch addLatch1 = new CountDownLatch(1);

		final DistributionRegionCallback callback2 = (e, r) -> { 
			if(root.getAllChildren().size() == 2) { 
				addLatch1.countDown(); 
			}
		};
		
		distributionRegionSyncer.registerCallback(callback2);
		final String regionPath = distributionRegionAdapter.getZookeeperPathForDistributionRegion(root);
		final long number = root.getHighestChildNumber() + 1;
		distributionRegionAdapter.createNewChild(regionPath, number, new Hyperrectangle(1d, 2d), GROUP);
		
		addLatch1.await();
		distributionRegionSyncer.unregisterCallback(callback2);
		
		System.out.println("=== Changing state");
		
		final CountDownLatch changeLatch = new CountDownLatch(1);
		final DistributionRegionCallback callback3 = (e, r) -> { 
			final boolean notAllChanged = root.getThisAndChildRegions()
				.stream()
				.anyMatch(a -> a.getState() != DistributionRegionState.ACTIVE_FULL);
			
			if(! notAllChanged) {
				changeLatch.countDown();
			}
		};
		distributionRegionSyncer.registerCallback(callback3);

		for(final DistributionRegion region : root.getThisAndChildRegions()) {
			distributionRegionAdapter.setStateForDistributionRegion(region, DistributionRegionState.ACTIVE_FULL);
		}
		
		changeLatch.await();
		
		distributionRegionSyncer.unregisterCallback(callback3);

		Assert.assertEquals(2, root.getAllChildren().size());
	}
	
	/**
	 * Build a new syncer
	 */
	private DistributionRegionSyncer buildSyncer() {
		final Set<DistributionRegionCallback> callbacks = new CopyOnWriteArraySet<>();
		final DistributionRegionIdMapper distributionRegionIdMapper = new DistributionRegionIdMapper(GROUP);
		
		final SpacePartitionerContext spacePartitionerContext = new SpacePartitionerContext(
				"", GROUP, ZookeeperClientFactory.getZookeeperClient(), 
				callbacks, distributionRegionIdMapper);
		
		return new DistributionRegionSyncer(spacePartitionerContext);
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
		
		final Hyperrectangle leftBoundingBox = root.getConveringBox().splitAndGetLeft(0, 0, true);
		final Hyperrectangle rightBoundingBox = root.getConveringBox().splitAndGetRight(0, 0, true);
		
		final String regionPath = distributionRegionAdapter.getZookeeperPathForDistributionRegion(root);
		
		final CountDownLatch latchLevel0 = new CountDownLatch(1);
		final DistributionRegionCallback level0Callback = (e, r) -> { if(root.getDirectChildren().size() == 2) { latchLevel0.countDown(); }};

		distributionRegionSyncer.registerCallback(level0Callback);
		distributionRegionAdapter.createNewChild(regionPath, 0, leftBoundingBox, GROUP);
		distributionRegionAdapter.createNewChild(regionPath, 1, rightBoundingBox, GROUP);
		latchLevel0.await();
		distributionRegionSyncer.unregisterCallback(level0Callback);
		
		Assert.assertEquals(2, root.getDirectChildren().size());
		Assert.assertTrue(root.getChildNumber(0) != null);
		Assert.assertTrue(root.getChildNumber(1) != null);
		
		return GROUP;
	}
}

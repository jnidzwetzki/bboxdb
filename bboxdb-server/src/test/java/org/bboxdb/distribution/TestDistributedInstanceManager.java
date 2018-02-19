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

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import org.bboxdb.commons.SystemInfo;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.bboxdb.distribution.membership.DistributedInstanceEvent;
import org.bboxdb.distribution.membership.ZookeeperBBoxDBInstanceAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperInstanceRegisterer;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDistributedInstanceManager {

	/**
	 * The name of the cluster for this test
	 */
	private static final String CLUSTER_NAME = "testcluster";

	/**
	 * Delete all old information on zookeeper
	 * 
	 * @throws ZookeeperException
	 */
	@Before
	public void before() throws ZookeeperException {
		ZookeeperClient zookeeperClient = getNewZookeeperClient(null);
		zookeeperClient.deleteCluster();
		zookeeperClient.shutdown();
	}
	
	/**
	 * test unregister
	 */
	@Test
	public void testRegisterUnregister() throws InterruptedException {
		final BBoxDBInstance instance = new BBoxDBInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		
		Assert.assertFalse(distributedInstanceManager.getInstances().isEmpty());
		Assert.assertEquals(1, distributedInstanceManager.getInstances().size());
		
		Assert.assertEquals(instance.getInetSocketAddress(), distributedInstanceManager.getInstances().get(0).getInetSocketAddress());
		
		zookeeperClient.shutdown();
		Assert.assertTrue(distributedInstanceManager.getInstances().isEmpty());
	}
	
	/**
	 * test unregister
	 */
	@Test
	public void testRegisterUnregister2() throws InterruptedException {
		final BBoxDBInstance instance = new BBoxDBInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		
		Assert.assertFalse(distributedInstanceManager.getInstances().isEmpty());
		Assert.assertEquals(1, distributedInstanceManager.getInstances().size());
		
		Assert.assertEquals(instance.getInetSocketAddress(), distributedInstanceManager.getInstances().get(0).getInetSocketAddress());
		
		zookeeperClient.shutdown();
		Assert.assertTrue(distributedInstanceManager.getInstances().isEmpty());
		
		final BBoxDBInstance instance2 = new BBoxDBInstance("node1:5050");
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient2);
		Assert.assertTrue(zookeeperClient2.isConnected());
		Thread.sleep(1000);
				
		Assert.assertFalse(distributedInstanceManager.getInstances().isEmpty());
		Assert.assertEquals(1, distributedInstanceManager.getInstances().size());
		
		Assert.assertEquals(instance.getInetSocketAddress(), distributedInstanceManager.getInstances().get(0).getInetSocketAddress());
		
		zookeeperClient2.shutdown();
	}
	
	/**
	 * No instance is known - but zookeeper init is called
	 */
	@Test
	public void testRegisterInstance1() throws InterruptedException {
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(null);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		Assert.assertTrue(BBoxDBInstanceManager.getInstance().getInstances().isEmpty());
		
		zookeeperClient.shutdown();
	}
	
	/**
	 * One instance is known
	 */
	@Test
	public void testRegisterInstance2() throws InterruptedException {
		final BBoxDBInstance instance = new BBoxDBInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		
		Assert.assertFalse(distributedInstanceManager.getInstances().isEmpty());
		Assert.assertEquals(1, distributedInstanceManager.getInstances().size());
		
		Assert.assertEquals(instance.getInetSocketAddress(), distributedInstanceManager.getInstances().get(0).getInetSocketAddress());
		
		zookeeperClient.shutdown();
	}
	
	/**
	 * One instance is known and changes
	 * @throws ZookeeperException 
	 */
	@Test
	public void testRegisterInstance3() throws InterruptedException, ZookeeperException {
		final BBoxDBInstance instance = new BBoxDBInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient);
	
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		Assert.assertTrue(BBoxDBInstanceState.FAILED == distributedInstanceManager.getInstances().get(0).getState());
	
		zookeeperBBoxDBInstanceAdapter.updateStateData(instance, BBoxDBInstanceState.OUTDATED);
		Thread.sleep(2000);
		System.out.println(distributedInstanceManager.getInstances());
		Assert.assertTrue(BBoxDBInstanceState.OUTDATED == distributedInstanceManager.getInstances().get(0).getState());
		
		zookeeperBBoxDBInstanceAdapter.updateStateData(instance, BBoxDBInstanceState.READY);
		Thread.sleep(1000);
		Assert.assertTrue(BBoxDBInstanceState.READY == distributedInstanceManager.getInstances().get(0).getState());
		
		zookeeperClient.shutdown();
	}
	
	/**
	 * Two instances are known and changing
	 * @throws ZookeeperException 
	 */
	@Test
	public void testRegisterInstance4() throws InterruptedException, ZookeeperException {
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		final BBoxDBInstance instance1 = new BBoxDBInstance("node1:5050");
		final BBoxDBInstance instance2 = new BBoxDBInstance("node2:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter1 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient1);
		
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter2 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient2);
		
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient1);

		Thread.sleep(1000);
		Assert.assertEquals(2, distributedInstanceManager.getInstances().size());

		Assert.assertTrue(BBoxDBInstanceState.FAILED == distributedInstanceManager.getInstances().get(0).getState());
		Assert.assertTrue(BBoxDBInstanceState.FAILED == distributedInstanceManager.getInstances().get(1).getState());

		// Change instance 1
		zookeeperBBoxDBInstanceAdapter1.updateStateData(instance1, BBoxDBInstanceState.OUTDATED);
		Thread.sleep(1000);
		for(final BBoxDBInstance instance : distributedInstanceManager.getInstances()) {
			if(instance.socketAddressEquals(instance1)) {
				Assert.assertTrue(instance.getState() == BBoxDBInstanceState.OUTDATED);
			} else {
				Assert.assertTrue(instance.getState() == BBoxDBInstanceState.FAILED);
			}
		}
		
		// Change instance 2
		zookeeperBBoxDBInstanceAdapter2.updateStateData(instance2, BBoxDBInstanceState.READY);
		Thread.sleep(2000);
		for(final BBoxDBInstance instance : distributedInstanceManager.getInstances()) {
			if(instance.socketAddressEquals(instance1)) {
				Assert.assertTrue(instance.getState() == BBoxDBInstanceState.OUTDATED);
			} else {
				Assert.assertTrue(instance.getState() == BBoxDBInstanceState.READY);
			}
		}
		
		zookeeperClient1.shutdown();
		zookeeperClient2.shutdown();
	}
	
	/**
	 * Test add event generation
	 * @throws InterruptedException 
	 * @throws ZookeeperException
	 */
	@Test(timeout=30000)
	public void testAddEvent() throws InterruptedException {
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		
		final BBoxDBInstance instance1 = new BBoxDBInstance("node4:5050");
		final BBoxDBInstance instance2 = new BBoxDBInstance("node5:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient1);
		Thread.sleep(5000);
		
		final CountDownLatch changedLatch = new CountDownLatch(1);

		distributedInstanceManager.registerListener(new BiConsumer<DistributedInstanceEvent, BBoxDBInstance>() {
			
			@Override
			public void accept(final DistributedInstanceEvent event, final BBoxDBInstance instance) {
				if(event == DistributedInstanceEvent.ADD) {
					if(instance.socketAddressEquals(instance2)) {
						changedLatch.countDown();
					}
				} else {
					// Unexpected event
					System.out.println("Got unexpeceted event: " + event);
					Assert.assertTrue(false);
				}				
			}
		});
		
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);
		
		changedLatch.await();
		
		distributedInstanceManager.removeAllListener();
		
		zookeeperClient1.shutdown();
		zookeeperClient2.shutdown();
	}
	
	/**
	 * Test changed event generation
	 * @throws InterruptedException 
	 * @throws ZookeeperException
	 */
	@Test(timeout=30000)
	public void testChangedEventOnChange() throws InterruptedException, ZookeeperException {
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		
		final BBoxDBInstance instance1 = new BBoxDBInstance("node6:5050");
		final BBoxDBInstance instance2 = new BBoxDBInstance("node7:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter1 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient1);
		
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient1);

		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter2 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient2);

		zookeeperBBoxDBInstanceAdapter1.updateStateData(instance1, BBoxDBInstanceState.READY);
		zookeeperBBoxDBInstanceAdapter2.updateStateData(instance2, BBoxDBInstanceState.READY);
		
		Thread.sleep(5000);
		
		final CountDownLatch changedLatch = new CountDownLatch(1);

		distributedInstanceManager.registerListener(new BiConsumer<DistributedInstanceEvent, BBoxDBInstance>() {
			
			@Override
			public void accept(final DistributedInstanceEvent event, final BBoxDBInstance instance) {

				if(event == DistributedInstanceEvent.CHANGED) {
					if(instance.socketAddressEquals(instance2)) {
						Assert.assertTrue(instance.getState() == BBoxDBInstanceState.OUTDATED);
						changedLatch.countDown();
					}
				} else {
					// Unexpected event
					Assert.assertTrue(false);
				}
			}
		});

		zookeeperBBoxDBInstanceAdapter2.updateStateData(instance2, BBoxDBInstanceState.OUTDATED);

		changedLatch.await();
		distributedInstanceManager.removeAllListener();
		
		zookeeperClient1.shutdown();
		zookeeperClient2.shutdown();
	}
	
	/**
	 * Test delete event generation
	 * @throws InterruptedException 
	 * @throws ZookeeperException
	 */
	@Test(timeout=30000)
	public void testChangedEventOnDeletion() throws InterruptedException, ZookeeperException {
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		
		final BBoxDBInstance instance1 = new BBoxDBInstance("node6:5050");
		final BBoxDBInstance instance2 = new BBoxDBInstance("node7:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter1 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient1);
		
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient1);
		
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter2 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient2);
		

		zookeeperBBoxDBInstanceAdapter1.updateStateData(instance1, BBoxDBInstanceState.READY);
		zookeeperBBoxDBInstanceAdapter2.updateStateData(instance2, BBoxDBInstanceState.READY);
		
		Thread.sleep(5000);
		
		final CountDownLatch changedLatch = new CountDownLatch(1);

		distributedInstanceManager.registerListener(new BiConsumer<DistributedInstanceEvent, BBoxDBInstance>() {
			
			@Override
			public void accept(final DistributedInstanceEvent event, final BBoxDBInstance instance) {

				if(event == DistributedInstanceEvent.CHANGED) {
					if(instance.socketAddressEquals(instance2)) {
						changedLatch.countDown();
					}
				} else {
					System.out.println("Got unexpeced event: " + event);
					// Unexpected event
					Assert.assertTrue(false);
				}
			}
		});
		
		zookeeperClient2.shutdown();
		
		changedLatch.await();
		distributedInstanceManager.removeAllListener();
		
		zookeeperClient1.shutdown();
	}
	
	/**
	 * Get a new instance of the zookeeper client
	 * @param instance
	 * @return 
	 */
	private static ZookeeperClient getNewZookeeperClient(final BBoxDBInstance instance) {
		
		final BBoxDBConfiguration scalephantConfiguration = 
				BBoxDBConfigurationManager.getConfiguration();
		
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();

		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeepernodes, CLUSTER_NAME);
		zookeeperClient.init();

		if(instance != null) {
			final ZookeeperInstanceRegisterer registerer 
				= new ZookeeperInstanceRegisterer(instance, zookeeperClient);
			registerer.init();
		}
	
		return zookeeperClient;
	}
	
	/**
	 * Test write the system info
	 */
	@Test
	public void testWriteSystemInfo() {
		final BBoxDBInstance instance1 = new BBoxDBInstance("node6:5050");
		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient1);
		
		final List<BBoxDBInstance> distributedInstances 
			= BBoxDBInstanceManager.getInstance().getInstances();
		
		Assert.assertEquals(1, distributedInstances.size());
		
		final BBoxDBInstance instance = distributedInstances.get(0);
		Assert.assertEquals(SystemInfo.getAvailableMemory(), instance.getMemory());
		Assert.assertEquals(SystemInfo.getCPUCores(), instance.getCpuCores());
		
		final BBoxDBConfiguration bboxDBConfiguration = BBoxDBConfigurationManager.getConfiguration();
		final List<String> directories = bboxDBConfiguration.getStorageDirectories();
		
		Assert.assertEquals(directories.size(), instance.getNumberOfStorages());
		Assert.assertEquals(directories.size(), instance.getAllTotalSpaceLocations().size());
		Assert.assertEquals(directories.size(), instance.getAllFreeSpaceLocations().size());
		
		for(final String directory : directories) {
			final File path = new File(directory);
			Assert.assertEquals(SystemInfo.getFreeDiskspace(path), 
					(long) instance.getAllFreeSpaceLocations().get(directory), 1000000);
			Assert.assertEquals(SystemInfo.getTotalDiskspace(path), 
					(long) instance.getAllTotalSpaceLocations().get(directory), 1000000);
		}

		zookeeperClient1.shutdown();
	}
	
	/**
	 * Test the reconnect
	 * @throws ZookeeperException 
	 */
	@Test
	public void testReconnect() throws InterruptedException, ZookeeperException {
		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();

		final BBoxDBInstance instance = new BBoxDBInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
		Assert.assertTrue(zookeeperClient.isConnected());
		Assert.assertEquals(1, distributedInstanceManager.getInstances().size());
		
		Thread.sleep(1000);
		
		// Disconnect
		zookeeperClient.shutdown();
		Assert.assertEquals(0, distributedInstanceManager.getInstances().size());

		// Reconnect
		Assert.assertFalse(zookeeperClient.isConnected());
		zookeeperClient.init();
		Assert.assertTrue(zookeeperClient.isConnected());

		// Wait for instances read
		Thread.sleep(1000);
		Assert.assertEquals(1, distributedInstanceManager.getInstances().size());
	}
}

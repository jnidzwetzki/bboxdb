/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.distribution;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceAddEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceChangedEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceEventCallback;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceState;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;

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
	 * No instance is known - but zookeeper init is called
	 */
	@Test
	public void testRegisterInstance1() throws InterruptedException {
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(null);
		zookeeperClient.startMembershipObserver();
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		Assert.assertTrue(DistributedInstanceManager.getInstance().getInstances().isEmpty());
		
		zookeeperClient.shutdown();
	}
	
	/**
	 * One instance is known
	 */
	@Test
	public void testRegisterInstance2() throws InterruptedException {
		final DistributedInstance instance = new DistributedInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		zookeeperClient.startMembershipObserver();
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		
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
		final DistributedInstance instance = new DistributedInstance("node1:5050");
		final ZookeeperClient zookeeperClient = getNewZookeeperClient(instance);
		zookeeperClient.startMembershipObserver();
		Assert.assertTrue(zookeeperClient.isConnected());
		Thread.sleep(1000);
		
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		Assert.assertTrue(DistributedInstanceState.UNKNOWN == distributedInstanceManager.getInstances().get(0).getState());
	
		zookeeperClient.setLocalInstanceState(DistributedInstanceState.READONLY);
		Thread.sleep(2000);
		System.out.println(distributedInstanceManager.getInstances());
		Assert.assertTrue(DistributedInstanceState.READONLY == distributedInstanceManager.getInstances().get(0).getState());
		
		zookeeperClient.setLocalInstanceState(DistributedInstanceState.READWRITE);
		Thread.sleep(1000);
		Assert.assertTrue(DistributedInstanceState.READWRITE == distributedInstanceManager.getInstances().get(0).getState());
		
		zookeeperClient.shutdown();
	}
	
	/**
	 * Two instances are known and changing
	 * @throws ZookeeperException 
	 */
	@Test
	public void testRegisterInstance4() throws InterruptedException, ZookeeperException {
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		final DistributedInstance instance1 = new DistributedInstance("node1:5050");
		final DistributedInstance instance2 = new DistributedInstance("node2:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);
		
		zookeeperClient1.startMembershipObserver();

		Thread.sleep(1000);
		Assert.assertEquals(2, distributedInstanceManager.getInstances().size());

		Assert.assertTrue(DistributedInstanceState.UNKNOWN == distributedInstanceManager.getInstances().get(0).getState());
		Assert.assertTrue(DistributedInstanceState.UNKNOWN == distributedInstanceManager.getInstances().get(1).getState());

		// Change instance 1
		zookeeperClient1.setLocalInstanceState(DistributedInstanceState.READONLY);
		Thread.sleep(1000);
		for(final DistributedInstance instance : distributedInstanceManager.getInstances()) {
			if(instance.socketAddressEquals(instance1)) {
				Assert.assertTrue(instance.getState() == DistributedInstanceState.READONLY);
			} else {
				Assert.assertTrue(instance.getState() == DistributedInstanceState.UNKNOWN);
			}
		}
		
		// Change instance 2
		zookeeperClient2.setLocalInstanceState(DistributedInstanceState.READWRITE);
		Thread.sleep(1000);
		for(final DistributedInstance instance : distributedInstanceManager.getInstances()) {
			if(instance.socketAddressEquals(instance1)) {
				Assert.assertTrue(instance.getState() == DistributedInstanceState.READONLY);
			} else {
				Assert.assertTrue(instance.getState() == DistributedInstanceState.READWRITE);
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
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		
		final DistributedInstance instance1 = new DistributedInstance("node4:5050");
		final DistributedInstance instance2 = new DistributedInstance("node5:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		zookeeperClient1.startMembershipObserver();
		Thread.sleep(5000);
		
		final CountDownLatch changedLatch = new CountDownLatch(1);

		distributedInstanceManager.registerListener(new DistributedInstanceEventCallback() {
			
			@Override
			public void distributedInstanceEvent(final DistributedInstanceEvent event) {

				if(event instanceof DistributedInstanceAddEvent) {
					if(event.getInstance().socketAddressEquals(instance2)) {
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
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		
		final DistributedInstance instance1 = new DistributedInstance("node6:5050");
		final DistributedInstance instance2 = new DistributedInstance("node7:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		zookeeperClient1.startMembershipObserver();
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);

		zookeeperClient1.setLocalInstanceState(DistributedInstanceState.READWRITE);
		zookeeperClient2.setLocalInstanceState(DistributedInstanceState.READWRITE);
		
		Thread.sleep(5000);
		
		final CountDownLatch changedLatch = new CountDownLatch(1);

		distributedInstanceManager.registerListener(new DistributedInstanceEventCallback() {
			
			@Override
			public void distributedInstanceEvent(final DistributedInstanceEvent event) {

				if(event instanceof DistributedInstanceChangedEvent) {
					if(event.getInstance().socketAddressEquals(instance2)) {
						Assert.assertTrue(event.getInstance().getState() == DistributedInstanceState.READONLY);
						changedLatch.countDown();
					}
				} else {
					// Unexpected event
					Assert.assertTrue(false);
				}
			}
		});
		
		zookeeperClient2.setLocalInstanceState(DistributedInstanceState.READONLY);

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
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		
		final DistributedInstance instance1 = new DistributedInstance("node6:5050");
		final DistributedInstance instance2 = new DistributedInstance("node7:5050");

		final ZookeeperClient zookeeperClient1 = getNewZookeeperClient(instance1);
		zookeeperClient1.startMembershipObserver();
		final ZookeeperClient zookeeperClient2 = getNewZookeeperClient(instance2);

		zookeeperClient1.setLocalInstanceState(DistributedInstanceState.READWRITE);
		zookeeperClient2.setLocalInstanceState(DistributedInstanceState.READWRITE);
		
		Thread.sleep(5000);
		
		final CountDownLatch changedLatch = new CountDownLatch(1);

		distributedInstanceManager.registerListener(new DistributedInstanceEventCallback() {
			
			@Override
			public void distributedInstanceEvent(final DistributedInstanceEvent event) {

				if(event instanceof DistributedInstanceChangedEvent) {
					if(event.getInstance().socketAddressEquals(instance2)) {
						changedLatch.countDown();
					}
				} else {
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
	private static ZookeeperClient getNewZookeeperClient(final DistributedInstance instance) {
		
		final ScalephantConfiguration scalephantConfiguration = 
				ScalephantConfigurationManager.getConfiguration();
		
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();

		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeepernodes, CLUSTER_NAME);
		
		if(instance != null) {
			zookeeperClient.registerScalephantInstanceAfterConnect(instance);
		}
		
		zookeeperClient.init();
		
		return zookeeperClient;
	}
}

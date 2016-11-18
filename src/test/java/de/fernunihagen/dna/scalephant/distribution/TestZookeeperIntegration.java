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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.distribution.DistributionGroupName;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegionFactory;
import de.fernunihagen.dna.scalephant.distribution.DistributionRegionWithZookeeperIntegration;
import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;

public class TestZookeeperIntegration {

	/**
	 * The zookeeper client
	 */
	protected static ZookeeperClient zookeeperClient;
	
	/**
	 * The name of the test region
	 */
	protected static final String TEST_GROUP = "4_abc";
	
	@BeforeClass
	public static void before() {
		final ScalephantConfiguration scalephantConfiguration 
			= ScalephantConfigurationManager.getConfiguration();
	
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();
		final String clustername = scalephantConfiguration.getClustername();

		System.out.println("Zookeeper nodes are: " + zookeepernodes);
		System.out.println("Zookeeper cluster is: " + clustername);
	
		zookeeperClient = new ZookeeperClient(zookeepernodes, clustername);
		zookeeperClient.init();
	}
	
	@AfterClass
	public static void after() {
		zookeeperClient.shutdown();
	}

	/**
	 * Test the id generation
	 * @throws ZookeeperException
	 */
	@Test
	public void testTableIdGenerator() throws ZookeeperException {
		final List<Integer> ids = new ArrayList<Integer>();
		
		for(int i = 0; i < 10; i++) {
			int nextId = zookeeperClient.getNextTableIdForDistributionGroup("mygroup1");
			System.out.println("The next id is: " + nextId);
			
			Assert.assertFalse(ids.contains(nextId));
			ids.add(nextId);
		}
	}
	
	/**
	 * Test the creation and the deletion of a distribution group
	 * @throws ZookeeperException
	 */
	@Test
	public void testDistributionGroupCreateDelete() throws ZookeeperException {
		
		// Create new group
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		final List<DistributionGroupName> groups = zookeeperClient.getDistributionGroups();
		System.out.println(groups);
		boolean found = false;
		for(final DistributionGroupName group : groups) {
			if(group.getFullname().equals(TEST_GROUP)) {
				found = true;
			}
		}
		
		Assert.assertTrue(found);
		Assert.assertTrue(zookeeperClient.isDistributionGroupRegistered(TEST_GROUP));
		
		// Delete group
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		final List<DistributionGroupName> groups2 = zookeeperClient.getDistributionGroups();
		found = false;
		for(final DistributionGroupName group : groups2) {
			if(group.getFullname().equals(TEST_GROUP)) {
				found = true;
			}
		}
		
		Assert.assertFalse(found);
		Assert.assertFalse(zookeeperClient.isDistributionGroupRegistered(TEST_GROUP));
	}
	
	/**
	 * Test the replication factor of a distribution group
	 * @throws ZookeeperException 
	 */
	@Test
	public void testDistributionGroupReplicationFactor() throws ZookeeperException {
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		Assert.assertEquals(3, zookeeperClient.getReplicationFactorForDistributionGroup(TEST_GROUP));
	}
	
	/**
	 * Test the split of a distribution region
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testDistributionRegionSplit() throws ZookeeperException, InterruptedException {
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		// Split and update
		final DistributionRegion distributionGroup = zookeeperClient.readDistributionGroup(TEST_GROUP);
		Assert.assertEquals(TEST_GROUP, distributionGroup.getName());
		
		Assert.assertEquals(DistributionRegion.STATE_ACTIVE, zookeeperClient.getStateForDistributionRegion(distributionGroup));
		distributionGroup.setSplit(10);
		
		Thread.sleep(1000);
		Assert.assertEquals(10.0, distributionGroup.getSplit(), 0.0001);
		Assert.assertEquals(DistributionRegion.STATE_SPLITTING, zookeeperClient.getStateForDistributionRegion(distributionGroup));

		// Reread group from zookeeper
		final DistributionRegion newDistributionGroup = zookeeperClient.readDistributionGroup(TEST_GROUP);
		Assert.assertEquals(10.0, newDistributionGroup.getSplit(), 0.0001);
	}
	
	/**
	 * Test the distribution of changes in the zookeeper structure (reading data from the second object)
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testDistributionRegionSplitWithZookeeperPropergate() throws ZookeeperException, InterruptedException {
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion distributionGroup1 = zookeeperClient.readDistributionGroup(TEST_GROUP);
		final DistributionRegion distributionGroup2 = zookeeperClient.readDistributionGroup(TEST_GROUP);

		// Update object 1
		distributionGroup1.setSplit(10);
		
		// Sleep 2 seconds to wait for the update
		Thread.sleep(2000);

		// Read update from the second object
		Assert.assertEquals(10.0, distributionGroup2.getSplit(), 0.0001);
	}
	
	/**
	 * Test the distribution of changes in the zookeeper structure (reading data from the second object)
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testDistributionRegionSplitWithZookeeperPropergate2() throws ZookeeperException, InterruptedException {
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion distributionGroup1 = zookeeperClient.readDistributionGroup(TEST_GROUP);
		final DistributionRegion distributionGroup2 = zookeeperClient.readDistributionGroup(TEST_GROUP);

		Assert.assertEquals(0, distributionGroup1.getLevel());
		
		// Update object 1
		distributionGroup1.setSplit(10);
		final DistributionRegion leftChild = distributionGroup1.getLeftChild();
		Assert.assertEquals(1, leftChild.getLevel());
		Assert.assertEquals(1, leftChild.getSplitDimension());
		leftChild.setSplit(50);
		
		// Sleep 2 seconds to wait for the update
		Thread.sleep(2000);

		// Read update from the second object
		Assert.assertEquals(10.0, distributionGroup2.getSplit(), 0.0001);
		Assert.assertEquals(50.0, distributionGroup2.getLeftChild().getSplit(), 0.0001);
	}
	
	/**
	 * Test the distribution group factory
	 */
	@Test
	public void testFactory() {
		DistributionRegionFactory.setZookeeperClient(null);
		final DistributionRegion region1 = DistributionRegionFactory.createRootRegion(TEST_GROUP);
		Assert.assertTrue(region1 instanceof DistributionRegion);
		Assert.assertFalse(region1 instanceof DistributionRegionWithZookeeperIntegration);

		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		final DistributionRegion region2 = DistributionRegionFactory.createRootRegion(TEST_GROUP);
		Assert.assertTrue(region2 instanceof DistributionRegionWithZookeeperIntegration);
	}
	
	/**
	 * Test the system register and unregister methods
	 * @throws ZookeeperException 
	 */
	@Test
	public void testSystemRegisterAndUnregister() throws ZookeeperException {
		final DistributedInstance systemName = new DistributedInstance("192.168.1.10:5050");
		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion region = zookeeperClient.readDistributionGroup(TEST_GROUP);
		final Collection<DistributedInstance> systems1 = zookeeperClient.getSystemsForDistributionRegion(region);
		Assert.assertEquals(0, systems1.size());
		
		// Add a system
		zookeeperClient.addSystemToDistributionRegion(region, systemName);
		final Collection<DistributedInstance> systems2 = zookeeperClient.getSystemsForDistributionRegion(region);
		Assert.assertEquals(1, systems2.size());
		Assert.assertTrue(systems2.contains(systemName));
		
		zookeeperClient.deleteSystemFromDistributionRegion(region, systemName);
		final Collection<DistributedInstance> systems3 = zookeeperClient.getSystemsForDistributionRegion(region);
		Assert.assertEquals(0, systems3.size());
	}
	
	/**
	 * Test the set and get checkpoint methods
	 * @throws ZookeeperException 
	 */
	@Test
	public void testSystemCheckpoint1() throws ZookeeperException {
		final DistributedInstance systemName1 = new DistributedInstance("192.168.1.10:5050");
		final DistributedInstance systemName2 = new DistributedInstance("192.168.1.20:5050");

		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion region = zookeeperClient.readDistributionGroup(TEST_GROUP);

		zookeeperClient.addSystemToDistributionRegion(region, systemName1);
		zookeeperClient.addSystemToDistributionRegion(region, systemName2);

		final long checkpoint1 = zookeeperClient.getCheckpointForDistributionRegion(region, systemName1);
		Assert.assertEquals(-1, checkpoint1);

		zookeeperClient.setCheckpointForDistributionRegion(region, systemName1, 5000);
		final long checkpoint2 = zookeeperClient.getCheckpointForDistributionRegion(region, systemName1);
		Assert.assertEquals(5000, checkpoint2);
		
		// System 2
		final long checkpoint3 = zookeeperClient.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(-1, checkpoint3);

		zookeeperClient.setCheckpointForDistributionRegion(region, systemName2, 9000);
		final long checkpoint4 = zookeeperClient.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(9000, checkpoint4);
		
		zookeeperClient.setCheckpointForDistributionRegion(region, systemName2, 9001);
		final long checkpoint5 = zookeeperClient.getCheckpointForDistributionRegion(region, systemName2);
		Assert.assertEquals(9001, checkpoint5);
	}
	
	/**
	 * Test the set and get checkpoint methods
	 * @throws ZookeeperException 
	 */
	@Test
	public void testSystemCheckpoint2() throws ZookeeperException {
		final DistributedInstance systemName1 = new DistributedInstance("192.168.1.10:5050");
		final DistributedInstance systemName2 = new DistributedInstance("192.168.1.20:5050");

		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion region = zookeeperClient.readDistributionGroup(TEST_GROUP);
		region.setSplit(50);
		
		zookeeperClient.addSystemToDistributionRegion(region.getLeftChild(), systemName1);
		zookeeperClient.addSystemToDistributionRegion(region.getLeftChild(), systemName2);
		zookeeperClient.addSystemToDistributionRegion(region.getRightChild(), systemName1);
		zookeeperClient.addSystemToDistributionRegion(region.getRightChild(), systemName2);
		
		zookeeperClient.setCheckpointForDistributionRegion(region.getLeftChild(), systemName1, 1);
		zookeeperClient.setCheckpointForDistributionRegion(region.getLeftChild(), systemName2, 2);
		zookeeperClient.setCheckpointForDistributionRegion(region.getRightChild(), systemName1, 3);
		zookeeperClient.setCheckpointForDistributionRegion(region.getRightChild(), systemName2, 4);

		final long checkpoint1 = zookeeperClient.getCheckpointForDistributionRegion(region.getLeftChild(), systemName1);
		final long checkpoint2 = zookeeperClient.getCheckpointForDistributionRegion(region.getLeftChild(), systemName2);
		final long checkpoint3 = zookeeperClient.getCheckpointForDistributionRegion(region.getRightChild(), systemName1);
		final long checkpoint4 = zookeeperClient.getCheckpointForDistributionRegion(region.getRightChild(), systemName2);

		Assert.assertEquals(1, checkpoint1);
		Assert.assertEquals(2, checkpoint2);
		Assert.assertEquals(3, checkpoint3);
		Assert.assertEquals(4, checkpoint4);
	}
	
	/**
	 * Test the systems field
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 */
	@Test
	public void testSystems() throws ZookeeperException, InterruptedException {
		final DistributedInstance systemName = new DistributedInstance("192.168.1.10:5050");
		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion region = zookeeperClient.readDistributionGroup(TEST_GROUP);
		zookeeperClient.addSystemToDistributionRegion(region, systemName);
		
		// Sleep 2 seconds to wait for the update
		Thread.sleep(2000);

		Assert.assertEquals(1, region.getSystems().size());
		Assert.assertTrue(region.getSystems().contains(systemName));
	}

	/**
	 * Test the generation of the nameprefix
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testNameprefix1() throws ZookeeperException, InterruptedException {
		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion region = zookeeperClient.readDistributionGroup(TEST_GROUP);
		Assert.assertEquals(0, region.getNameprefix());
	}
	
	/**
	 * Test the generation of the nameprefix
	 * @throws ZookeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testNameprefix2() throws ZookeeperException, InterruptedException {
		DistributionRegionFactory.setZookeeperClient(zookeeperClient);
		zookeeperClient.deleteDistributionGroup(TEST_GROUP);
		zookeeperClient.createDistributionGroup(TEST_GROUP, (short) 3); 
		
		final DistributionRegion region = zookeeperClient.readDistributionGroup(TEST_GROUP);
		region.setSplit(10);
		final DistributionRegion leftChild = region.getLeftChild();
		final DistributionRegion rightChild = region.getRightChild();
		
		Assert.assertEquals(0, region.getNameprefix());
		Assert.assertEquals(1, leftChild.getNameprefix());
		Assert.assertEquals(2, rightChild.getNameprefix());
	}
}

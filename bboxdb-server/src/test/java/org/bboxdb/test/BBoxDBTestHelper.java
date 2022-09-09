/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.test;

import java.util.HashSet;
import java.util.Set;

import org.bboxdb.commons.service.ServiceState;
import org.bboxdb.distribution.allocator.ResourceAllocationException;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.bboxdb.distribution.membership.ZookeeperBBoxDBInstanceAdapter;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.junit.Assert;
import org.mockito.Mockito;

public class BBoxDBTestHelper {
	
	/**
	 * The mocked connection
	 */
	public final static BBoxDBConnection MOCKED_CONNECTION = Mockito.mock(BBoxDBConnection.class);
	
	static {
		final ServiceState state = new ServiceState();
		state.dipatchToStarting();
		state.dispatchToRunning();
		Mockito.when(MOCKED_CONNECTION.isConnected()).thenReturn(true);
		Mockito.when(MOCKED_CONNECTION.getConnectionState()).thenReturn(state);
	}
	
	/**
	 * Register some fake instance in zookeeper
	 * @return
	 * @throws ZookeeperException
	 */
	public static void registerFakeInstance(final int fakeInstances) throws ZookeeperException {
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		
		final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter 
			= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient);
		
		final int basePort = 10000;
		
		final Set<BBoxDBInstance> instances = new HashSet<>();

		for(int i = 0; i < fakeInstances; i++) {
			final int port = basePort + i;
			final BBoxDBInstance instance = new BBoxDBInstance("localhost:" + port, BBoxDBInstanceState.READY);
			zookeeperBBoxDBInstanceAdapter.updateNodeInfo(instance);
			zookeeperBBoxDBInstanceAdapter.updateStateData(instance);
			instances.add(instance);
		}
				
		// Register instances
		BBoxDBInstanceManager.getInstance().updateInstanceList(instances);
	}
	
	/**
	 * Wait until the space paritioner is reread
	 * @param distributionGroup
	 * @param desiredVersion
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	public static void waitForSpacepartitonerUpdate(final String distributionGroup, 
			final long desiredVersion) throws BBoxDBException, InterruptedException {
		
		for(int i = 0; i < 10; i++) {
			 SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroup);
			
			 final long readVersion = SpacePartitionerCache
					.getInstance().getSpacePartitionerVersion(distributionGroup);
			 
			 if(readVersion == desiredVersion) {
				 break;
			 }
			 
			 if(i < 10) {
				 Thread.sleep(1000);
			 } else {
				 Assert.fail("Unable to get the correct space partitioner version " 
						 + desiredVersion + " / " + readVersion);
			 }
		}
	}
	
	/**
	 * Recreate the given distribution group
	 * @param distributionGroupZookeeperAdapter 
	 * @param configuration
	 * @param fakeInstances 
	 * @throws ZookeeperException
	 * @throws BBoxDBException
	 * @throws ResourceAllocationException
	 * @throws InterruptedException
	 */
	public static void recreateDistributionGroup(DistributionGroupAdapter distributionGroupZookeeperAdapter, final String distributionGroup,
			final DistributionGroupConfiguration configuration, final int fakeInstances)
			throws ZookeeperException, BBoxDBException, ResourceAllocationException, InterruptedException {
		
		distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
	
		distributionGroupZookeeperAdapter.deleteDistributionGroup(distributionGroup);
		
		// Add fake instances for testing
		registerFakeInstance(fakeInstances);
		
		final long createdVersion = 
				distributionGroupZookeeperAdapter.createDistributionGroup(distributionGroup, configuration);
				
		SpacePartitionerCache.getInstance().resetSpacePartitioner(distributionGroup);
		
		waitForSpacepartitonerUpdate(distributionGroup, createdVersion);
	}
}

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
package org.bboxdb.storage.util;

import java.io.File;
import java.util.Set;

import org.bboxdb.commons.io.FileUtil;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.junit.Assert;

public class EnvironmentHelper {
	
	/** 
	 * Hard reset test environment
	 * @throws ZookeeperException
	 */
	public static void resetTestEnvironment() throws ZookeeperException {
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();

		zookeeperClient.deleteCluster();
		zookeeperClient.createDirectoryStructureIfNeeded();
		
		final Set<String> groups = SpacePartitionerCache.getInstance().getAllKnownDistributionGroups();
		
		for(final String group : groups) {
			SpacePartitionerCache.getInstance().resetSpacePartitioner(group);
		}
		
		String storageDir0 = BBoxDBConfigurationManager.getConfiguration().getStorageDirectories().get(0);
		final File relationDirectory = new File(storageDir0);
		FileUtil.deleteRecursive(relationDirectory.toPath());
		
		final File dataDir = new File(SSTableHelper.getDataDir(storageDir0));
		
		dataDir.mkdirs();		
	}
	
	/**
	 * Recreate the given distribution group
	 * @param client
	 * @param DISTRIBUTION_GROUP
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	public static void recreateDistributionGroup(final BBoxDB client, final String DISTRIBUTION_GROUP) throws InterruptedException, BBoxDBException {
		
		// Delete distribution group
		final EmptyResultFuture resultDelete = client.deleteDistributionGroup(DISTRIBUTION_GROUP);
		resultDelete.waitForCompletion();
		Assert.assertFalse(resultDelete.isFailed());
		
		// Create distribution group
		final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder.create(2)
				.withReplicationFactor((short) 1)
				.build();
		
		final EmptyResultFuture resultCreate = client.createDistributionGroup(DISTRIBUTION_GROUP, 
				configuration);
		
		resultCreate.waitForCompletion();
		Assert.assertFalse(resultCreate.isFailed());
	}
}

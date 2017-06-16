/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.SSTableFlushCallback;
import org.bboxdb.storage.entity.SSTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableFlushZookeeperAdapter implements SSTableFlushCallback {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableFlushZookeeperAdapter.class);
	
	@Override
	public void flushCallback(final SSTableName ssTableName, final long flushTimestamp) {
		
		// Fetch the local instance
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		final DistributedInstance localInstance = ZookeeperClientFactory.getLocalInstanceName(configuration);
	
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForTableName(
					ssTableName, zookeeperClient);

			final DistributionRegion distributionGroupRoot = distributionAdapter.getRootNode();
			
			final DistributionRegion distributionRegion = DistributionRegionHelper.getDistributionRegionForNamePrefix(distributionGroupRoot, ssTableName.getRegionId());
		
			
			logger.debug("Updating checkpoint for: {} to {}", ssTableName.getFullname(), flushTimestamp);
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			
			if(distributionGroupZookeeperAdapter != null && distributionRegion != null) {
				distributionGroupZookeeperAdapter.setCheckpointForDistributionRegion(distributionRegion, localInstance, flushTimestamp);
			}
			
		} catch (ZookeeperException | BBoxDBException e) {
			
			if(Thread.currentThread().isInterrupted()) {
				return;
			}
			
			logger.warn("Unable to find distribution region: " , e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}
}

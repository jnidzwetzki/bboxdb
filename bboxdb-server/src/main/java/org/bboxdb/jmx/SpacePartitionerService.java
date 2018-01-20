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
package org.bboxdb.jmx;

import javax.management.MBeanException;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.RegionSplitter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpacePartitionerService implements SpacePartitionerServiceMBean {

	/**
	 * The storage registry
	 */
	private final TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SpacePartitionerService.class);

	public SpacePartitionerService(final TupleStoreManagerRegistry storageRegistry) {
		this.storageRegistry = storageRegistry;
	}

	@Override
	public String getName() {
		return "Space partitioner service bean";
	}

	@Override
	public void splitDistributionRegion(final String distributionGroup, final long regionId) 
			throws MBeanException {
		
		logger.info("Split the region {} in group {}", regionId, distributionGroup);
		
		try {
			final RegionSplitter regionSplitter = new RegionSplitter();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache
					.getSpacePartitionerForGroupName(distributionGroup);
			
			final DistributionRegion region = spacePartitioner.getRootNode();
			final DistributionRegion regionToSplit 
				= DistributionRegionHelper.getDistributionRegionForNamePrefix(region, regionId);

			final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();

			final boolean isOnLocalSystem = regionToSplit.getSystems()
					.stream()
					.map(r -> r.getInetSocketAddress())
					.anyMatch(r -> r.equals(localInstance.getInetSocketAddress()));
			
			if(! isOnLocalSystem) {
				throw new BBoxDBException("Unable to split region, this system is not responsible: " 
						+ regionToSplit.getSystems());
			}
			
			regionSplitter.splitRegion(regionToSplit, spacePartitioner, storageRegistry);
			
			logger.info("Split the region {} in group {} - DONE", regionId, distributionGroup);
		} catch (Exception e) {
			throw new MBeanException(e);
		}
	}

}

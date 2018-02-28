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
package org.bboxdb.distribution.partitioner;

import java.util.Set;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;

public class QuadtreeSpacePartitioner implements SpacePartitioner {

	@Override
	public void init(String spacePartitionerConfig, DistributionGroupName distributionGroupName,
			ZookeeperClient zookeeperClient, DistributionGroupZookeeperAdapter distributionGroupAdapter) 
					throws ZookeeperException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DistributionRegion getRootNode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void allocateSystemsToRegion(DistributionRegion region, Set<BBoxDBInstance> allocationSystems)
			throws ZookeeperException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void splitRegion(DistributionRegion regionToSplit, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) throws BBoxDBException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean registerCallback(DistributionRegionChangedCallback callback) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean unregisterCallback(DistributionRegionChangedCallback callback) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isMergingSupported(final DistributionRegion distributionRegion) {
		return ! distributionRegion.isLeafRegion();
	}
	
	@Override
	public boolean isSplittingSupported(final DistributionRegion distributionRegion) {
		return distributionRegion.isLeafRegion();
	}

	@Override
	public void prepareMerge(final DistributionRegion regionToMerge) 
			throws BBoxDBException {
		
		throw new IllegalArgumentException("Unable to merge region, this is not supported");
	}
	
	@Override
	public void mergeComplete(final DistributionRegion regionToMerge) throws BBoxDBException {
		throw new IllegalArgumentException("Unable to merge region, this is not supported");
	}

	@Override
	public DistributionRegionIdMapper getDistributionRegionIdMapper() {
		// TODO Auto-generated method stub
		return null;
	}
}

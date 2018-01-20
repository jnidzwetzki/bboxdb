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
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;

public interface SpacePartitioner {
	
	/**
	 * All dependencies are set, init the partitioner
	 * @param spacePartitionerConfig 
	 * @throws ZookeeperException
	 */
	public void init(final String spacePartitionerConfig, 
			final DistributionGroupName distributionGroupName, 
			final ZookeeperClient zookeeperClient, 
			final DistributionGroupZookeeperAdapter distributionGroupAdapter) throws ZookeeperException;

	/**
	 * Get the root node
	 * @return
	 */
	public DistributionRegion getRootNode();
	
	/**
	 * Allocate systems to a new region
	 * @param region
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 * @throws ZookeeperNotFoundException
	 */
	public void allocateSystemsToRegion(final DistributionRegion region, 
			final Set<BBoxDBInstance> allocationSystems) throws ZookeeperException;
	
	/**
	 * Split the node on the given position
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 * @throws ZookeeperNotFoundException
	 * @throws BBoxDBException 
	 */
	public void splitRegion(final DistributionRegion regionToSplit, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) throws BBoxDBException;
	
	/**
	 * Merge the given region
	 * @param regionToMerge
	 * @throws BBoxDBException
	 */
	public void prepareMerge(final DistributionRegion regionToMerge) throws BBoxDBException;
	
	/**
	 * Merging of the region is done 
	 * @param regionToMerge
	 * @throws BBoxDBException
	 */
	public void mergeComplete(final DistributionRegion regionToMerge) throws BBoxDBException;
	
	/**
	 * Is the merging of regions supported?
	 * @return
	 */
	public boolean isMergingSupported();
	
	/**
	 * Register a changed callback
	 * @param callback
	 * @return
	 */
	public boolean registerCallback(final DistributionRegionChangedCallback callback);
	
	/**
	 * Remove a changed callback
	 * @param callback
	 * @return
	 */
	public boolean unregisterCallback(final DistributionRegionChangedCallback callback);

}

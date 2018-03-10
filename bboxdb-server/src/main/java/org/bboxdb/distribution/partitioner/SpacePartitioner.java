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

import java.util.Collection;
import java.util.List;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;

public interface SpacePartitioner {
	
	/**
	 * All dependencies are set, init the partitioner
	 * @param spacePartitionerContext
	 * @throws ZookeeperException
	 */
	public void init(final SpacePartitionerContext spacePartitionerContext) throws BBoxDBException;
	
	/**
	 * Create the root node
	 * @param configuration 
	 * @throws BBoxDBException 
	 */
	public void createRootNode(final DistributionGroupConfiguration configuration) throws BBoxDBException;

	/**
	 * Get the root node
	 * @return
	 * @throws BBoxDBException 
	 */
	public DistributionRegion getRootNode() throws BBoxDBException;
	
	/**
	 * Split the region and return the newly crated child regions
	 * 
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 * @throws ZookeeperNotFoundException
	 * @throws BBoxDBException 
	 */
	public List<DistributionRegion> splitRegion(final DistributionRegion regionToSplit, 
			final Collection<BoundingBox> samples) throws BBoxDBException;
	
	/**
	 * A split is complete
	 * @param regionToSplit
	 * @throws BBoxDBException
	 */
	public void splitComplete(final DistributionRegion regionToSplit) throws BBoxDBException;
	
	/**
	 * A split is failed
	 * @param regionToSplit
	 * @throws BBoxDBException
	 */
	public void splitFailed(final DistributionRegion regionToSplit) throws BBoxDBException;
	
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
	 * Merging of the region is done 
	 * @param regionToMerge
	 * @throws BBoxDBException
	 */
	public void mergeFailed(final DistributionRegion regionToMerge) throws BBoxDBException;
	
	
	/**
	 * Is the merging of regions supported?
	 * @return
	 */
	public boolean isMergingSupported(final DistributionRegion distributionRegion);
	
	/**
	 * Is the splitting of the region supported?
	 * @param distributionRegion
	 * @return
	 */
	public boolean isSplittingSupported(final DistributionRegion distributionRegion);
	
	/**
	 * Register a changed callback
	 * @param callback
	 * @return
	 */
	public boolean registerCallback(final DistributionRegionCallback callback);
	
	/**
	 * Remove a changed callback
	 * @param callback
	 * @return
	 */
	public boolean unregisterCallback(final DistributionRegionCallback callback);
	
	/**
	 * Get the region id mapper
	 */
	public DistributionRegionIdMapper getDistributionRegionIdMapper();
	
	/**
	 * Shutdown the space partitioner
	 */
	public void shutdown();

}

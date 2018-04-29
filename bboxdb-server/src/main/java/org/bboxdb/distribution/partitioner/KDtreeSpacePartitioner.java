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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingBasedSplitStrategy;
import org.bboxdb.distribution.partitioner.regionsplit.SplitpointStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class KDtreeSpacePartitioner extends AbstractTreeSpacePartitoner {

	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KDtreeSpacePartitioner.class);

	/**
	 * Split the node at the given position
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException 
	 * @throws ZookeeperNotFoundException 
	 */
	@Override
	public List<DistributionRegion> splitRegion(final DistributionRegion regionToSplit, 
			final Collection<Hyperrectangle> samples) throws BBoxDBException {
		
		try {
			final SplitpointStrategy splitpointStrategy = new SamplingBasedSplitStrategy(samples);
			
			final int splitDimension = getSplitDimension(regionToSplit);
			final Hyperrectangle regionBox = regionToSplit.getConveringBox();
			final double splitPosition = splitpointStrategy.getSplitPoint(splitDimension, regionBox);
			
			splitNode(regionToSplit, splitPosition);
			
			return regionToSplit.getDirectChildren();
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Split the node at the given split point
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws BBoxDBException
	 * @throws ResourceAllocationException 
	 */
	public void splitNode(final DistributionRegion regionToSplit, final double splitPosition)
			throws BBoxDBException, ResourceAllocationException {
		
		try {
			logger.debug("Write split at pos {} into zookeeper", splitPosition);
			final String parentPath 
				= distributionRegionZookeeperAdapter.getZookeeperPathForDistributionRegion(regionToSplit);
			
			// Calculate the covering bounding boxes
			final Hyperrectangle parentBox = regionToSplit.getConveringBox();
			final int splitDimension = getSplitDimension(regionToSplit);
			final Hyperrectangle leftBoundingBox = parentBox.splitAndGetLeft(splitPosition, splitDimension, true);
			final Hyperrectangle rightBoundingBox = parentBox.splitAndGetRight(splitPosition, splitDimension, false);
						
			// Only one system executes the split, therefore we can determine the child ids
			distributionRegionZookeeperAdapter.createNewChild(parentPath, 0, leftBoundingBox, distributionGroupName);
			distributionRegionZookeeperAdapter.createNewChild(parentPath, 1, rightBoundingBox, distributionGroupName);
			
			// Update state
			distributionRegionZookeeperAdapter.setStateForDistributionGroup(parentPath, DistributionRegionState.SPLITTING);
			
			waitUntilChildrenAreCreated(regionToSplit, 2);
			allocateSystems(regionToSplit, 2);
			setStateToRedistributionActiveAndWait(regionToSplit, 2);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new BBoxDBException(e);
		} 
	}
	
	/**
	 * Get the dimension of the distribution region
	 * @return
	 */
	private int getDimension() {
		try {
			final DistributionGroupConfigurationCache instance = DistributionGroupConfigurationCache.getInstance();
			final DistributionGroupConfiguration config = instance.getDistributionGroupConfiguration(distributionGroupName);
			return config.getDimensions();
		} catch (ZookeeperNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Returns the dimension of the split
	 * @return
	 */
	@VisibleForTesting
	public int getSplitDimension(final DistributionRegion distributionRegion) {
		return distributionRegion.getLevel() % getDimension();
	}
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingBasedSplitStrategy;
import org.bboxdb.distribution.partitioner.regionsplit.SplitpointStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionSyncerHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;

public class DynamicgridSpacePartitioner extends AbstractGridSpacePartitioner {

	@Override
	public boolean isSplitable(final DistributionRegion distributionRegion) {
		return distributionRegion.getState() == DistributionRegionState.ACTIVE;
	}

	@Override
	public List<DistributionRegion> splitRegion(final DistributionRegion regionToSplit, 
			final Collection<BoundingBox> samples) throws BBoxDBException {
		
		try {
			final SplitpointStrategy splitpointStrategy = new SamplingBasedSplitStrategy(samples);
			
			final BoundingBox regionBox = regionToSplit.getConveringBox();
			final double splitPosition = splitpointStrategy.getSplitPoint(0, regionBox);
			
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
			final DistributionRegion parent = regionToSplit.getParent();
			
			final String sourcePath 
				= distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(regionToSplit);

			final String parentPath 
				= distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(parent);
			
			// Calculate the covering bounding boxes
			final BoundingBox parentBox = regionToSplit.getConveringBox();
			final BoundingBox leftBoundingBox = parentBox.splitAndGetLeft(splitPosition, 0, true);
			final BoundingBox rightBoundingBox = parentBox.splitAndGetRight(splitPosition, 0, false);
			
			final String fullname = distributionGroupName;
			
			// Only one system executes the split, therefore we can determine the child ids
			final int oldNumberOfhildren = parent.getDirectChildren().size();
			final long childNumber = parent.getHighestChildNumber();
			
			final String child1Path = distributionGroupZookeeperAdapter.createNewChild(parentPath, 
					(int) childNumber + 1, leftBoundingBox, fullname);
			
			final String child2Path = distributionGroupZookeeperAdapter.createNewChild(parentPath, 
					(int) childNumber + 2, rightBoundingBox, fullname);

			// Update state
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(sourcePath, DistributionRegionState.SPLITTING);
			
			final Predicate<DistributionRegion> predicate = (r) -> r.getDirectChildren().size() == oldNumberOfhildren + 2;
			DistributionRegionSyncerHelper.waitForPredicate(predicate, regionToSplit, distributionRegionSyncer);
						
			// The first child is stored on the same systems as the parent
			SpacePartitionerHelper.copySystemsToRegion(regionToSplit.getSystems(), 
					child1Path, distributionGroupZookeeperAdapter);

			final List<BBoxDBInstance> blacklistSystems = regionToSplit.getSystems();
			
			SpacePartitionerHelper.allocateSystemsToRegion(child2Path, fullname, 
					blacklistSystems, distributionGroupZookeeperAdapter);

		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}

	@Override
	public void splitComplete(final DistributionRegion sourceRegion, 
			final List<DistributionRegion> destination) throws BBoxDBException {
		
		try {
			final DistributionGroupZookeeperAdapter zookeperAdapter 
				= ZookeeperClientFactory.getDistributionGroupAdapter();
			
			logger.info("Split done deleting: {}", sourceRegion.getIdentifier());
			distributionGroupZookeeperAdapter.deleteChild(sourceRegion);
						
			// Children are ready
			for(final DistributionRegion childRegion : destination) {
				zookeperAdapter.setStateForDistributionRegion(childRegion, DistributionRegionState.ACTIVE);
			}
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}
	
	@Override
	public DistributionRegion getDestinationForMerge(final List<DistributionRegion> source) 
			throws BBoxDBException {
		
		try {			
			assert(source.size() == 2) : "We can only merge 2 regions";
			
			final BoundingBox bbox = BoundingBox.getCoveringBox(
					source.get(0).getConveringBox(), 
					source.get(1).getConveringBox());
			
			final DistributionRegion parent = source.get(0).getParent();
			
			final String parentPath = distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(parent);
			
			final int childNumber = (int) parent.getHighestChildNumber();
			
			final String childPath = distributionGroupZookeeperAdapter.createNewChild(parentPath, 
					childNumber, bbox, distributionGroupName);
			
			SpacePartitionerHelper.allocateSystemsToRegion(childPath, distributionGroupName, 
					new ArrayList<>(), distributionGroupZookeeperAdapter);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.ACTIVE);
	
			return distributionGroupZookeeperAdapter.getNodeForPath(source.get(0).getRootRegion(), childPath);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		}
	}


	@Override
	public void mergeComplete(final List<DistributionRegion> source, final DistributionRegion destination) 
			throws BBoxDBException {
		
		try {			
			for(final DistributionRegion childRegion : source) {
				logger.info("Merge done deleting: {}", childRegion.getIdentifier());
				distributionGroupZookeeperAdapter.deleteChild(childRegion);
			}
			
			distributionGroupZookeeperAdapter.setStateForDistributionRegion(destination, 
					DistributionRegionState.ACTIVE);
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}

	@Override
	public void mergeFailed(final List<DistributionRegion> source, final DistributionRegion destination) 
			throws BBoxDBException {
		
		try {
			distributionGroupZookeeperAdapter.deleteChild(destination);
			
			for(final DistributionRegion childRegion : source) {
				distributionGroupZookeeperAdapter.setStateForDistributionRegion(childRegion, 
						DistributionRegionState.ACTIVE);
				
			}
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}		
	}

	@Override
	public List<List<DistributionRegion>> getMergeCandidates(final DistributionRegion distributionRegion) {
		
		final List<List<DistributionRegion>> result = new ArrayList<>();
		
		final List<DistributionRegion> children = distributionRegion.getParent().getDirectChildren();
		children.sort((e1, e2) -> 
			e1.getConveringBox().getIntervalForDimension(0)
			.compareTo(e2.getConveringBox().getIntervalForDimension(0)));
		
		for(int pos = 0; pos < children.size(); pos++) {
			final DistributionRegion region = children.get(pos);
			
			// Use the left and the right region as merge candidates
			if(region.equals(distributionRegion)) {
				if(pos - 1 >= 0) {
					result.add(Arrays.asList(distributionRegion, children.get(pos - 1)));
				}
				
				if(pos + 1 < children.size()) {
					result.add(Arrays.asList(distributionRegion, children.get(pos + 1)));
				}
				
				break;
			}
		}
		
		return result;
	}

	/**
	 * Create the cell grid
	 * @param splitConfig
	 * @param configuration 
	 * @param rootPath 
	 * @param rootBox 
	 * @throws Exception
	 */
	protected void createCells(final String[] splitConfig, final DistributionGroupConfiguration configuration, 
			final String rootPath, final BoundingBox rootBox) throws Exception {
				
		createGridInDimension(splitConfig, rootPath, rootBox, configuration.getDimensions() - 1);	
	}
	
	/**
	 * Create the cell grid
	 * @param splitConfig
	 * @param parentPath
	 * @param box
	 * @param dimension
	 * @throws ZookeeperException
	 * @throws InputParseException
	 * @throws ZookeeperNotFoundException
	 * @throws ResourceAllocationException
	 */
	private void createGridInDimension(final String[] splitConfig, 
			final String parentPath, final BoundingBox box, final int dimension) 
					throws ZookeeperException, InputParseException, ZookeeperNotFoundException, ResourceAllocationException {
		
		logger.info("Processing dimension {}", dimension);
				
		if(dimension == 0) {	
			final String childPath = distributionGroupZookeeperAdapter.createNewChild(parentPath, 
					0, box, distributionGroupName);
			
			SpacePartitionerHelper.allocateSystemsToRegion(childPath, distributionGroupName, 
					new ArrayList<BBoxDBInstance>(), distributionGroupZookeeperAdapter);
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.ACTIVE);
			
			return;
		}

		final String stepIntervalString = splitConfig[dimension];
		
		final double stepInterval 
			= MathUtil.tryParseDouble(stepIntervalString, () -> "Unable to parse" + stepIntervalString);
			
		BoundingBox boundingBoxToSplit = box;
		int childNumber = 0;
		
		while(boundingBoxToSplit != null) {
			final double splitPos = boundingBoxToSplit.getCoordinateLow(dimension) + stepInterval;
			BoundingBox nodeBox;
			
			if(splitPos >= boundingBoxToSplit.getCoordinateHigh(dimension)) {
				nodeBox = boundingBoxToSplit;
				boundingBoxToSplit = null;
			} else {
				nodeBox = boundingBoxToSplit.splitAndGetRight(splitPos, dimension, false);
				boundingBoxToSplit = boundingBoxToSplit.splitAndGetRight(splitPos, dimension, true);
			}
						
			final String childPath = distributionGroupZookeeperAdapter.createNewChild(parentPath, 
					childNumber, nodeBox, distributionGroupName);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.SPLIT);
			createGridInDimension(splitConfig, childPath, nodeBox, dimension - 1);
			
			childNumber++;
		}
	}
	
	/**
	 * Check the config parameter
	 */
	protected void checkConfigParameter(final DistributionGroupConfiguration configuration, 
			final String[] splitConfig) throws BBoxDBException {
		
		final int dimensions = configuration.getDimensions();
		
		// n-1 dimensions - bounding box
		final int dimensionSizes = splitConfig.length;
		
		if(dimensionSizes != dimensions) {
			throw new BBoxDBException("Got invalid configuration (invlid amount of grid sizes " 
					+ dimensions + " / " + dimensionSizes + ")");
		}
	}
}

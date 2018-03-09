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
import java.util.Collection;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedgridSpacePartitioner extends AbstractSpacePartitioner {

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FixedgridSpacePartitioner.class);

	
	@Override
	public void createRootNode(final DistributionGroupConfiguration configuration) throws BBoxDBException {
				
		// [[0.0,5.0]:[0.0,5.0]];0.5;0.5
		final String spConfig = spacePartitionerContext.getSpacePartitionerConfig();
		
		if(spConfig.isEmpty()) {
			throw new BBoxDBException("Got empty space partitioner config");
		}
		
		final String[] splitConfig = spConfig.split(";");
		
		final int dimensions = configuration.getDimensions();
		final int dimensionSizes = splitConfig.length -1;
		
		if(dimensionSizes != dimensions) {
			throw new BBoxDBException("Got invalid configuration (invlid amount of grid sizes " 
					+ dimensions + " / " + dimensionSizes + ")");
		}
		
		try {
			final String distributionGroup 
				= spacePartitionerContext.getDistributionGroupName().getFullname();
			
			final String rootPath = 
					distributionGroupZookeeperAdapter.getDistributionGroupRootElementPath(distributionGroup);
			
			zookeeperClient.createDirectoryStructureRecursive(rootPath);
			
			final int nameprefix = distributionGroupZookeeperAdapter
					.getNextTableIdForDistributionGroup(distributionGroup);
						
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
					Integer.toString(nameprefix).getBytes());
			
			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
					"".getBytes());
					
			final BoundingBox rootBox = new BoundingBox(splitConfig[0]);
			distributionGroupZookeeperAdapter.setBoundingBoxForPath(rootPath, rootBox);
			
			// Create grid
			createGrid(splitConfig, configuration, rootPath, rootBox);

			zookeeperClient.createPersistentNode(rootPath + "/" + ZookeeperNodeNames.NAME_REGION_STATE, 
					DistributionRegionState.SPLIT.getStringValue().getBytes());		
			
			distributionGroupZookeeperAdapter.markNodeMutationAsComplete(rootPath);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		}
	}

	/**
	 * Create the cell grid
	 * @param splitConfig
	 * @param configuration 
	 * @param rootPath 
	 * @param rootBox 
	 * @throws InputParseException 
	 * @throws ZookeeperException 
	 * @throws ResourceAllocationException 
	 * @throws ZookeeperNotFoundException 
	 */
	private void createGrid(final String[] splitConfig, final DistributionGroupConfiguration configuration, 
			final String rootPath, final BoundingBox rootBox) throws ZookeeperException, 
				InputParseException, ZookeeperNotFoundException, ResourceAllocationException {
				
		createGridInDimension(splitConfig, rootPath, rootBox, configuration.getDimensions() - 1);	
	}

	/**
	 * Create the grid in the given dimension
	 * @param splitConfig
	 * @param rootPath 
	 * @param rootBox 
	 * @param dimension
	 * @throws ZookeeperException 
	 * @throws InputParseException 
	 * @throws ResourceAllocationException 
	 * @throws ZookeeperNotFoundException 
	 */
	private void createGridInDimension(final String[] splitConfig, 
			final String parentPath, final BoundingBox box, final int dimension) 
					throws ZookeeperException, InputParseException, ZookeeperNotFoundException, ResourceAllocationException {
		
		logger.info("Processing dimension {}", dimension);

		final String fullname = distributionGroupName.getFullname();
		final String stepIntervalString = splitConfig[dimension + 1];
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
					childNumber, nodeBox, fullname);
			
			if(dimension == 0) {	
				SpacePartitionerHelper.allocateSystemsToRegion(childPath, distributionGroupName.getFullname(), 
						new ArrayList<BBoxDBInstance>(), distributionGroupZookeeperAdapter);
				distributionGroupZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.ACTIVE);
			} else {
				distributionGroupZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.SPLIT);
				createGridInDimension(splitConfig, childPath, nodeBox, dimension - 1);
			}
			
			childNumber++;
		}
	}

	@Override
	public void splitRegion(DistributionRegion regionToSplit, Collection<BoundingBox> samples) throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");
	}

	@Override
	public void splitComplete(DistributionRegion regionToSplit) throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");	
	}

	@Override
	public void splitFailed(DistributionRegion regionToSplit) throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");
	}

	@Override
	public void prepareMerge(DistributionRegion regionToMerge) throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");
	}

	@Override
	public void mergeComplete(DistributionRegion regionToMerge) throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");
	}

	@Override
	public void mergeFailed(DistributionRegion regionToMerge) throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");	
	}

	@Override
	public boolean isMergingSupported(DistributionRegion distributionRegion) {
		return false;
	}

	@Override
	public boolean isSplittingSupported(DistributionRegion distributionRegion) {
		return false;
	}
	
}

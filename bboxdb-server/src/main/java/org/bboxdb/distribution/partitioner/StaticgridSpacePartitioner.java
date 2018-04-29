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
import java.util.List;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticgridSpacePartitioner extends AbstractGridSpacePartitioner {

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(StaticgridSpacePartitioner.class);

	/**
	 * Create the cell grid
	 * @param splitConfig
	 * @param configuration 
	 * @param rootPath 
	 * @param rootBox 
	 * @throws Exception
	 */
	protected void createCells(final String[] splitConfig, final DistributionGroupConfiguration configuration, 
			final String rootPath, final Hyperrectangle rootBox) throws Exception {
				
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
			final String parentPath, final Hyperrectangle box, final int dimension) 
					throws ZookeeperException, InputParseException, ZookeeperNotFoundException, ResourceAllocationException {
		
		logger.info("Processing dimension {}", dimension);

		final String fullname = distributionGroupName;
		final String stepIntervalString = splitConfig[dimension + 1];
		final double stepInterval 
			= MathUtil.tryParseDouble(stepIntervalString, () -> "Unable to parse" + stepIntervalString);
			
		Hyperrectangle boundingBoxToSplit = box;
		int childNumber = 0;
		
		while(boundingBoxToSplit != null) {
			final double splitPos = boundingBoxToSplit.getCoordinateLow(dimension) + stepInterval;
			Hyperrectangle nodeBox;
			
			if(splitPos >= boundingBoxToSplit.getCoordinateHigh(dimension)) {
				nodeBox = boundingBoxToSplit;
				boundingBoxToSplit = null;
			} else {
				nodeBox = boundingBoxToSplit.splitAndGetRight(splitPos, dimension, false);
				boundingBoxToSplit = boundingBoxToSplit.splitAndGetRight(splitPos, dimension, true);
			}
						
			final String childPath = distributionRegionZookeeperAdapter.createNewChild(parentPath, 
					childNumber, nodeBox, fullname);
			
			if(dimension == 0) {	
				SpacePartitionerHelper.allocateSystemsToRegion(childPath, distributionGroupName, 
						new ArrayList<BBoxDBInstance>(), zookeeperClient);
				distributionRegionZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.ACTIVE);
			} else {
				distributionRegionZookeeperAdapter.setStateForDistributionGroup(childPath, DistributionRegionState.SPLIT);
				createGridInDimension(splitConfig, childPath, nodeBox, dimension - 1);
			}
			
			childNumber++;
		}
	}

	@Override
	public List<DistributionRegion> splitRegion(final DistributionRegion regionToSplit, 
			final Collection<Hyperrectangle> samples) throws BBoxDBException {
		
		throw new BBoxDBException("Unsupported operation");
	}

	@Override
	public List<List<DistributionRegion>> getMergeCandidates(final DistributionRegion distributionRegion) {
		
		// Merging is not supported
		return new ArrayList<>();
	}

	@Override
	public boolean isSplitable(final DistributionRegion distributionRegion) {
		return false;
	}

	@Override
	public void splitComplete(final DistributionRegion sourceRegion, final List<DistributionRegion> destination)
			throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");			
	}

	@Override
	public void splitFailed(final DistributionRegion sourceRegion, final List<DistributionRegion> destination)
			throws BBoxDBException {
		throw new BBoxDBException("Unsupported operation");			
	}

	@Override
	public void mergeComplete(final List<DistributionRegion> source, final DistributionRegion destination) 
			throws BBoxDBException {
		
		throw new BBoxDBException("Unsupported operation");			
	}

	@Override
	public void mergeFailed(final List<DistributionRegion> source, final DistributionRegion destination) 
			throws BBoxDBException {
		
		throw new BBoxDBException("Unsupported operation");		
	}

	@Override
	public DistributionRegion getDestinationForMerge(final List<DistributionRegion> source) 
			throws BBoxDBException {
		
		throw new BBoxDBException("Unsupported operation");	
	}

	@Override
	public void prepareMerge(final List<DistributionRegion> source, 
			final DistributionRegion destination) throws BBoxDBException {
		
		throw new BBoxDBException("Unsupported operation");	
	}
	
	/**
	 * Check the config parameter
	 */
	protected void checkConfigParameter(final DistributionGroupConfiguration configuration, 
			final String[] splitConfig) throws BBoxDBException {
		
		final int dimensions = configuration.getDimensions();
		final int dimensionSizes = splitConfig.length - 1;
		
		if(dimensionSizes != dimensions) {
			throw new BBoxDBException("Got invalid configuration (invlid amount of grid sizes " 
					+ dimensions + " / " + dimensionSizes + ")");
		}
	}
}

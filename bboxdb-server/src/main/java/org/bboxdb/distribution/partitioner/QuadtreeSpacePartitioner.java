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

import org.bboxdb.commons.ListHelper;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuadtreeSpacePartitioner extends AbstractTreeSpacePartitoner {

	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QuadtreeSpacePartitioner.class);

	@Override
	public List<DistributionRegion> splitRegion(final DistributionRegion regionToSplit, 
			final Collection<Hyperrectangle> samples) throws BBoxDBException {
		
		try {
			logger.info("Splitting region {}", regionToSplit.getIdentifier());
			
			final String parentPath 
				= distributionRegionZookeeperAdapter.getZookeeperPathForDistributionRegion(regionToSplit);
	
			final Hyperrectangle box = regionToSplit.getConveringBox();
			
			final List<Hyperrectangle> childBoxes = createBoundingBoxes(box);
			
			final int numberOfChilden = childBoxes.size();
	
			for(int i = 0; i < numberOfChilden; i++) {
				final Hyperrectangle childBox = childBoxes.get(i);
				distributionRegionZookeeperAdapter.createNewChild(parentPath, i, childBox, distributionGroupName);
			}
			
			// Update state
			distributionRegionZookeeperAdapter.setStateForDistributionGroup(parentPath, DistributionRegionState.SPLITTING);
			
			waitUntilChildrenAreCreated(regionToSplit, numberOfChilden);
			allocateSystems(regionToSplit, numberOfChilden);
			setStateToRedistributionActiveAndWait(regionToSplit, numberOfChilden);
			
			return regionToSplit.getDirectChildren();
		} catch (ZookeeperException | ZookeeperNotFoundException | ResourceAllocationException e) {
			throw new BBoxDBException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Create a list with bounding boxes
	 * @param box
	 * @return
	 */
	private List<Hyperrectangle> createBoundingBoxes(final Hyperrectangle box) {
		
		final List<DoubleInterval> list1 = new ArrayList<>();
		final List<DoubleInterval> list2 = new ArrayList<>();
		generateIntervalLists(box, list1, list2);
		
		List<List<DoubleInterval>> intervalCombinations = ListHelper.getCombinations(list1, list2);
		
		final List<Hyperrectangle> childBoxes = new ArrayList<>();
		
		for(List<DoubleInterval> boxAsIntervals : intervalCombinations) {
			childBoxes.add(new Hyperrectangle(boxAsIntervals));
		}
		
		return childBoxes;
	}

	/**
	 * Generate the interval lists
	 * @param box
	 * @param list1
	 * @param list2
	 */
	private void generateIntervalLists(final Hyperrectangle box, final List<DoubleInterval> list1,
			final List<DoubleInterval> list2) {
		
		for(int dimension = 0; dimension < box.getDimension(); dimension++) {
			final DoubleInterval interval = box.getIntervalForDimension(dimension);
			final double midpoint = interval.getMidpoint();
			final DoubleInterval interval1 = interval.splitAndGetLeftPart(midpoint, false);
			list1.add(interval1);
			final DoubleInterval interval2 = interval.splitAndGetRightPart(midpoint, true);
			list2.add(interval2);
		}
	}
}

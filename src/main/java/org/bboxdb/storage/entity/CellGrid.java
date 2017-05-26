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
package org.bboxdb.storage.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

public class CellGrid {
	
	/**
	 * The covering box
	 */
	protected BoundingBox coveringBox;
	
	/**
	 * The cells per dimension
	 */
	protected double cellsPerDimension;
	
	/**
	 * All boxes of this grid
	 */
	protected Set<BoundingBox> allBoxes;

	public CellGrid(final BoundingBox coveringBox, final double cellsPerDimension) {
		
		if(cellsPerDimension <= 0) {
			throw new IllegalArgumentException("Number of cells has to be > 0");
		}
		
		this.coveringBox = Objects.requireNonNull(coveringBox);
		this.cellsPerDimension = Objects.requireNonNull(cellsPerDimension);
		this.allBoxes = new HashSet<>();
		
		createBBoxesForDimension(coveringBox);
	}
	
	/**
	 * Get all cell bounding boxes that are covered by this box
	 * @param boundingBox
	 * @return
	 */
	public Set<BoundingBox> getAllInersectedBoundingBoxes(final BoundingBox boundingBox) {
		
		if(coveringBox.getDimension() != boundingBox.getDimension()){
			throw new IllegalArgumentException("Dimension of the cell is: " + coveringBox.getDimension() 
			+ " of the query object " + boundingBox.getDimension());
		}
		
		return allBoxes
				.stream()
				.filter(b -> b.overlaps(boundingBox))
				.collect(Collectors.toSet());
	}
	
	
	/**
	 * Create the cell boxes, recursive
	 * @param bbox
	 * @param dimension
	 */
	protected void createBBoxesForDimension(final BoundingBox bbox) {
		
		final List<List<DoubleInterval>> cells = new ArrayList<>();
		
		// Generate all possible interval for each dimension
		for(int dimension = 0; dimension < bbox.getDimension(); dimension++) {
			final DoubleInterval baseInterval = bbox.getIntervalForDimension(dimension);
			final double cellsSize = baseInterval.getLength() / (double) cellsPerDimension;

			// List of intervals for this dimension
			final List<DoubleInterval> intervals = new ArrayList<>();
			cells.add(intervals);
			
			for(int offset = 0; offset < cellsPerDimension; offset++) {
				double end = baseInterval.getBegin() + ((offset+1) * cellsSize);
				double begin = baseInterval.getBegin() + (offset * cellsSize);
				
				// The last cell contains the end point
				final boolean endIncluded = (offset + 1 == cellsPerDimension); 
				
				final DoubleInterval interval = new DoubleInterval(begin, end, true, endIncluded);
				intervals.add(interval);
			}
		}
		
		convertListsToBoxes(cells);
	}

	/**
	 * Convert the lists of intervals to bounding boxes
	 * @param cells
	 */
	protected void convertListsToBoxes(final List<List<DoubleInterval>> cells) {
		final List<List<DoubleInterval>> intervallProduct = Lists.cartesianProduct(cells);
		
		for(List<DoubleInterval> intervalls : intervallProduct) {
			final BoundingBox boundingBox = new BoundingBox(intervalls);
			allBoxes.add(boundingBox);
		}
	}
	
	/**
	 * Get all cells of the grid
	 * @return
	 */
	public Set<BoundingBox> getAllCells() {
		return Collections.unmodifiableSet(allBoxes);
	}

}

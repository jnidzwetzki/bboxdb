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
package org.bboxdb.storage.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder;

import com.google.common.collect.Lists;

public class CellGrid {
	
	/**
	 * The covering box
	 */
	protected BoundingBox coveringBox;
	
	/**
	 * All boxes of this grid
	 */
	protected final Set<BoundingBox> allBoxes;
	
	/**
	 * The spatial index
	 */
	protected final RTreeBuilder spatialIndexBuilder = new RTreeBuilder();

	/**
	 * Build the grid with fixed cell size
	 * @param coveringBox
	 * @param cellsPerDimension
	 * @return
	 */
	public static CellGrid buildWithFixedCellSize(final BoundingBox coveringBox, 
			final double cellSize) {
		
		Objects.requireNonNull(coveringBox);
		Objects.requireNonNull(cellSize);
		
		if(cellSize <= 0) {
			throw new IllegalArgumentException("Cell size has to be > 0");
		}
		
		// Fixed size
		return createCells(coveringBox, (d) -> cellSize);
	}
	
	/**
	 * Build the grid with a fixed amount of cells
	 * @param coveringBox
	 * @param cellsPerDimension
	 * @return
	 */
	public static CellGrid buildWithFixedAmountOfCells(final BoundingBox coveringBox, 
			final double cellsPerDimension) {
		
		Objects.requireNonNull(coveringBox);
		Objects.requireNonNull(cellsPerDimension);
		
		if(cellsPerDimension <= 0) {
			throw new IllegalArgumentException("Number of cells has to be > 0");
		}
		
		// Fixed amount of cells
		return createCells(coveringBox, 
				(d) -> coveringBox.getIntervalForDimension(d).getLength() / cellsPerDimension);
	}

	/**
	 * Create the cell intervals for the grid
	 * @param coveringBox
	 * @param cellSizeInDimension
	 * @return
	 */
	private static CellGrid createCells(final BoundingBox coveringBox,
			final Function<Integer, Double> cellSizeInDimension) {
		
		final List<List<DoubleInterval>> cells = new ArrayList<>();
		
		// Generate all possible interval for each dimension
		for(int dimension = 0; dimension < coveringBox.getDimension(); dimension++) {
			final DoubleInterval baseInterval = coveringBox.getIntervalForDimension(dimension);
			final double cellSize = cellSizeInDimension.apply(dimension);
			final int cellsInDimension = (int) Math.ceil(baseInterval.getLength() / cellSize);

			if(cellsInDimension <= 0) {
				throw new IllegalArgumentException("Cells in dimension " + (dimension + 1) + " has to be > 0");
			}
			
			// List of intervals for this dimension
			final List<DoubleInterval> intervals = new ArrayList<>();
			cells.add(intervals);
			
			for(int offset = 0; offset < cellsInDimension; offset++) {
				final double begin = baseInterval.getBegin() + (offset * cellSize);
				final double end = Math.min(
						baseInterval.getBegin() + ((offset+1) * cellSize),
						baseInterval.getEnd());
				
				// The last cell contains the end point
				final boolean endIncluded = (offset + 1 == cellsInDimension); 
				
				final DoubleInterval interval = new DoubleInterval(begin, end, true, endIncluded);
				intervals.add(interval);
			}
		}
		
		final Set<BoundingBox> allBoxes = convertListsToBoxes(cells);
		return new CellGrid(coveringBox, allBoxes);
	}

	/**
	 * The private constructor
	 * @param coveringBox
	 * @param allBoxes
	 */
	private CellGrid(final BoundingBox coveringBox, final Set<BoundingBox> allBoxes) {
		this.coveringBox = Objects.requireNonNull(coveringBox);
		this.allBoxes = allBoxes;		

		// Build spatial index
		final List<SpatialIndexEntry> indexEntries = allBoxes
				.stream()
				.map(b -> new SpatialIndexEntry(b, 1))
				.collect(Collectors.toList());
		
		spatialIndexBuilder.bulkInsert(indexEntries);
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
		
		final List<? extends SpatialIndexEntry> entries = spatialIndexBuilder.getEntriesForRegion(boundingBox);
		
		return entries.stream().map(e -> e.getBoundingBox()).collect(Collectors.toSet());
	}
	
	/**
	 * Convert the lists of intervals to bounding boxes
	 * @param cells
	 * @return 
	 */
	protected static Set<BoundingBox> convertListsToBoxes(final List<List<DoubleInterval>> cells) {
		final Set<BoundingBox> allBoxes = new HashSet<>();
		final List<List<DoubleInterval>> intervallProduct = Lists.cartesianProduct(cells);
		
		for(List<DoubleInterval> intervalls : intervallProduct) {
			final BoundingBox boundingBox = new BoundingBox(intervalls);
			allBoxes.add(boundingBox);
		}
		
		return allBoxes;
	}
	
	/**
	 * Get all cells of the grid
	 * @return
	 */
	public Set<BoundingBox> getAllCells() {
		return Collections.unmodifiableSet(allBoxes);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((allBoxes == null) ? 0 : allBoxes.hashCode());
		result = prime * result + ((coveringBox == null) ? 0 : coveringBox.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CellGrid other = (CellGrid) obj;
		if (allBoxes == null) {
			if (other.allBoxes != null)
				return false;
		} else if (!allBoxes.equals(other.allBoxes))
			return false;
		if (coveringBox == null) {
			if (other.coveringBox != null)
				return false;
		} else if (!coveringBox.equals(other.coveringBox))
			return false;
		return true;
	}

}

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
package org.bboxdb.tools;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.CellGrid;
import org.junit.Assert;
import org.junit.Test;

public class TestCellGrid {

	/**
	 * Test the empty grid
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testEmptyGrid() {
		final CellGrid cellGrid = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(1.0, 1.0, 1.0, 1.0), 10);
		final Hyperrectangle fullBox = Hyperrectangle.createFullCoveringDimensionBoundingBox(2);
		cellGrid.getAllInersectedBoundingBoxes(fullBox);
	}
	
	/**
	 * Test negative cell size
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testNegativeSize() {
		CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(1.0, 1.0, 1.0, 1.0), -10);
	}
	
	/**
	 * Test the empty grid
	 */
	@Test(expected=RuntimeException.class)
	public void testInvalidParameter1() {
		CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(1.0, 1.0, 1.0, 1.0), -1);
	}
	
	/**
	 * Test the empty grid
	 */
	@Test(expected=RuntimeException.class)
	public void testInvalidParameter2() {
		CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(1.0, 1.0, 1.0, 1.0), 0);
	}
	
	/**
	 * Test the empty grid
	 */
	@Test(expected=RuntimeException.class)
	public void testInvalidParameter3() {
		CellGrid.buildWithFixedAmountOfCells(null, 1.0);
	}
	
	/**
	 * Test the wrong dimension
	 */
	@Test(expected=RuntimeException.class)
	public void testWrongDimension() {
		final CellGrid cellGrid = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(1.0, 1.0, 1.0, 1.0), 1);
		cellGrid.getAllInersectedBoundingBoxes(new Hyperrectangle(1.0, 1.0));		
	}
	
	/**
	 * Test the cell creation
	 */
	@Test(timeout=60000)
	public void testGetCells1() {
		final CellGrid cellGrid1D = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0), 10);
		Assert.assertEquals(10, cellGrid1D.getAllCells().size());

		final CellGrid cellGrid2D = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 10);
		Assert.assertEquals(100, cellGrid2D.getAllCells().size());

		final CellGrid cellGrid3D = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 10);
		Assert.assertEquals(1000, cellGrid3D.getAllCells().size());
		
		final CellGrid cellGrid4D = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 10);
		Assert.assertEquals(10000, cellGrid4D.getAllCells().size());
	}
	
	/**
	 * Test the cell creation
	 */
	@Test(timeout=60000)
	public void testGetCells2() {
		final CellGrid cellGrid2D = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 10);
		Assert.assertEquals(1, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(1.5, 1.5, 1.5, 1.5)).size());
		
		// End pos
		Assert.assertEquals(1, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(10.0, 10.0, 10.0, 10.0)).size());
		
		// Outside
		Assert.assertEquals(0, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(10.1, 10.1, 10.1, 10.1)).size());
		
		// Grid point
		Assert.assertEquals(1, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(1.0, 1.0, 1.0, 1.0)).size());
		
		// Start point
		Assert.assertEquals(1, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(0.0, 0.0, 0.0, 0.0)).size());
	}
	
	/**
	 * Test the cell creation
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetCells3() {
		final CellGrid cellGrid2D = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 10);
		Assert.assertEquals(1, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(1.5, 1.5, 1.5, 1.5)).size());
		
		// Wrong dimension
		Assert.assertEquals(1, cellGrid2D.getAllInersectedBoundingBoxes(new Hyperrectangle(10.0, 10.0)).size());
	}
	
	/**
	 * Test cell grid creation
	 */
	@Test(timeout=60000)
	public void testCellGridCreation() {
		final CellGrid cellGrid2D1 = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 10);
		final CellGrid cellGrid2D2 = CellGrid.buildWithFixedCellSize(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 1);

		Assert.assertEquals(cellGrid2D1.getAllCells(), cellGrid2D2.getAllCells());
		Assert.assertEquals(cellGrid2D1, cellGrid2D2);
	}
	
	/**
	 * Hashcode
	 */
	@Test(timeout=60000)
	public void testHashCodeAndEquals() {
		final CellGrid cellGrid2D1 = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 10);
		final CellGrid cellGrid2D2 = CellGrid.buildWithFixedAmountOfCells(new Hyperrectangle(0.0, 10.0, 0.0, 10.0), 10);
		Assert.assertEquals(cellGrid2D1, cellGrid2D2);
		Assert.assertEquals(cellGrid2D1.hashCode(), cellGrid2D2.hashCode());
	}
}

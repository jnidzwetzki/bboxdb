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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bboxdb.storage.entity.BoundingBox;
import org.junit.Assert;
import org.junit.Test;

public class TestBoundingBox {
	
	protected final static float EQUALS_DELTA = 0.001f;

	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid1() {
		final BoundingBox bb1 = new BoundingBox(1d, 2d, 3d, 4d, 5d);
	}
	
	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid2() {
		// Dimension 1 error
		final BoundingBox bb2 = new BoundingBox(2d, -2d, 3d, 4d);
	}
	
	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid3() {	
		// Dimension 2 error
		final BoundingBox bb3 = new BoundingBox(1d, 2d, 3d, -4d);
	}
	
	/**
	 * Create some valid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test
	public void testBoundingBoxCreateValid() {
		final BoundingBox bb1 = new BoundingBox(1d, 10d);
		final BoundingBox bb2 = new BoundingBox(-10d, 10d);
		final BoundingBox bb3 = new BoundingBox(1d, 20d, -50d, 50d);
		final BoundingBox bb4 = new BoundingBox(1d, 20d, -50d, 50d, -100d, 10d);
	}
	
	/**
	 * Test the getter and the setter for the dimension data
	 */
	@Test
	public void testGetValues() {
		final BoundingBox bb1 = new BoundingBox(1d, 11d);
		Assert.assertEquals(1d, bb1.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(10d, bb1.getExtent(0), EQUALS_DELTA);
		
		final BoundingBox bb2 = new BoundingBox(1d, 21d, -50d, 0d, -100d, -90d);
		Assert.assertEquals(1d, bb2.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(20d, bb2.getExtent(0), EQUALS_DELTA);
		Assert.assertEquals(-50d, bb2.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(50d, bb2.getExtent(1), EQUALS_DELTA);
		Assert.assertEquals(-100d, bb2.getCoordinateLow(2), EQUALS_DELTA);
		Assert.assertEquals(10d, bb2.getExtent(2), EQUALS_DELTA);
	}
	
	/**
	 * Test the calculation of low and high values
	 */
	@Test
	public void testLowHigh() {
		final BoundingBox bb1 = new BoundingBox(1d, 11d);
		Assert.assertEquals(1d, bb1.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(11d, bb1.getCoordinateHigh(0), EQUALS_DELTA);

		final BoundingBox bb2 = new BoundingBox(1d, 11d, 10d, 60d);
		Assert.assertEquals(1d, bb2.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(11d, bb2.getCoordinateHigh(0), EQUALS_DELTA);
		Assert.assertEquals(10d, bb2.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(60d, bb2.getCoordinateHigh(1), EQUALS_DELTA);
	}
	
	/**
	 * Test the dimension of the bounding box
	 */
	@Test
	public void testDimension() {
		final BoundingBox bb1 = new BoundingBox();
		Assert.assertEquals(0, bb1.getDimension());
		
		final BoundingBox bb2 = new BoundingBox(1d, 10d);
		Assert.assertEquals(1, bb2.getDimension());
		
		final BoundingBox bb3 = new BoundingBox(1d, 10d, 10d, 50d);
		Assert.assertEquals(2, bb3.getDimension());
		
		final BoundingBox bb4 = new BoundingBox(1d, 10d, 10d, 50d, 10d, 10d);
		Assert.assertEquals(3, bb4.getDimension());
	}
	
	/**
	 * Test the overlapping in 1d
	 */
	@Test
	public void testOverlapping1D() {
		final BoundingBox bb1left = new BoundingBox(0d, 10d);
		final BoundingBox bb1middle = new BoundingBox(5d, 15d);
		final BoundingBox bb1right = new BoundingBox(10.1d, 20.1d);
		
		Assert.assertTrue(bb1left.overlaps(bb1left));
		Assert.assertTrue(bb1middle.overlaps(bb1middle));
		Assert.assertTrue(bb1right.overlaps(bb1right));
		
		Assert.assertFalse(bb1left.overlaps(null));
		Assert.assertFalse(bb1middle.overlaps(null));
		Assert.assertFalse(bb1right.overlaps(null));
		
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));

		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		Assert.assertTrue(bb1middle.overlaps(bb1right));
		Assert.assertTrue(bb1right.overlaps(bb1middle));		
	}
	
	/**
	 * Test the overlapping in 2d
	 */
	@Test
	public void testOverlapping2D() {
		final BoundingBox bb1left = new BoundingBox(0d, 1d, 0d, 1d);
		final BoundingBox bb1leftinside = new BoundingBox(0.5d, 0.7d, 0.5d, 0.7d);
		
		Assert.assertTrue(bb1left.overlaps(bb1leftinside));
		Assert.assertTrue(bb1leftinside.overlaps(bb1left));

		final BoundingBox bb1middle = new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d);
		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		final BoundingBox bb1right = new BoundingBox(1d, 2d, 10d, 11d);
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));
	}
	
	/**
	 * Test the overlapping in 3d
	 */
	@Test
	public void testOverlapping3D() {
		final BoundingBox bb1left = new BoundingBox(0d, 1d, 0d, 1d, 0d, 1d);
		final BoundingBox bb1leftinside = new BoundingBox(0.5d, 0.7d, 0.5d, 0.7d, 0.5d, 0.7d);
		Assert.assertTrue(bb1left.overlaps(bb1leftinside));
		Assert.assertTrue(bb1leftinside.overlaps(bb1left));
		
		final BoundingBox bb1middle = new BoundingBox(0.5d, 1.5d, 0.5d, 1.5d, 0.5d, 1.5d);
		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		final BoundingBox bb1right = new BoundingBox(10d, 11d, 10d, 11d, 10d, 11d);
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));
	}
	
	/**
	 * Test empty bounding box overlapping
	 */
	@Test
	public void testOverlapEmptyBoundingBox() {
		final BoundingBox bb1left = new BoundingBox(0d, 1d, 0d, 1d, 0d, 1d);
		Assert.assertTrue(bb1left.overlaps(BoundingBox.EMPTY_BOX));
		Assert.assertTrue(BoundingBox.EMPTY_BOX.overlaps(BoundingBox.EMPTY_BOX));
	}
	
	/**
	 * Test the creation of the covering bounding box 
	 */
	@Test
	public void testCoverBoundingBox1() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 1d, 3d);
		final BoundingBox boundingBox2 = new BoundingBox(1d, 4d, 1d, 4d);
		
		final BoundingBox boundingBox3 = new BoundingBox(1d, 4d, 1d, 4d, 1d, 4d);
		final BoundingBox boundingBox4 = new BoundingBox(-1d, 2d, -1d, 2d, -1d, 2d);

		final BoundingBox boundingBoxResult1 = BoundingBox.getCoveringBox();
		Assert.assertEquals(BoundingBox.EMPTY_BOX, boundingBoxResult1);
		
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(boundingBox1));
		Assert.assertEquals(boundingBox2, BoundingBox.getCoveringBox(boundingBox2));
		Assert.assertEquals(boundingBox3, BoundingBox.getCoveringBox(boundingBox3));
		Assert.assertEquals(boundingBox4, BoundingBox.getCoveringBox(boundingBox4));
		
		final BoundingBox boundingBoxResult2 = BoundingBox.getCoveringBox(boundingBox1, boundingBox2);
		Assert.assertEquals(2, boundingBoxResult2.getDimension());
		Assert.assertEquals(boundingBoxResult2, boundingBox2);

		final BoundingBox boundingBoxResult4 = BoundingBox.getCoveringBox(boundingBox3, boundingBox4);
		Assert.assertEquals(3, boundingBoxResult4.getDimension());		
		Assert.assertEquals(-1.0d, boundingBoxResult4.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(4.0d, boundingBoxResult4.getCoordinateHigh(0), EQUALS_DELTA);
		Assert.assertEquals(-1.0d, boundingBoxResult4.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(4.0d, boundingBoxResult4.getCoordinateHigh(1), EQUALS_DELTA);
		Assert.assertEquals(-1.0d, boundingBoxResult4.getCoordinateLow(2), EQUALS_DELTA);
		Assert.assertEquals(4.0d, boundingBoxResult4.getCoordinateHigh(2), EQUALS_DELTA);
	}
	
	/**
	 * Test the creation of the covering bounding box 
	 */
	@Test
	public void testCoverBoundingBox2() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 1d, 3d);
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(boundingBox1));
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(boundingBox1, null));
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(null, boundingBox1));
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(boundingBox1, BoundingBox.EMPTY_BOX));
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(BoundingBox.EMPTY_BOX, boundingBox1, BoundingBox.EMPTY_BOX));
		Assert.assertEquals(boundingBox1, BoundingBox.getCoveringBox(BoundingBox.EMPTY_BOX, null, boundingBox1, BoundingBox.EMPTY_BOX));
	}
	
	/**
	 * Merge two boxes with wrong dimension
	 */
	@Test(expected=IllegalArgumentException.class)
	public void mergeBoxesWithWrongDimension1() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 1d, 3d);		
		final BoundingBox boundingBox2 = new BoundingBox(1d, 4d, 1d, 4d, 1d, 4d);
		
		final BoundingBox boundingBoxResult3 = BoundingBox.getCoveringBox(boundingBox1, boundingBox2);
		Assert.assertTrue(boundingBoxResult3 == null);
	}
	
	/**
	 * Merge two boxes with wrong dimension
	 */
	@Test(expected=IllegalArgumentException.class)
	public void mergeBoxesWithWrongDimension2() {
		
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 1d, 3d);
		final BoundingBox boundingBox2 = new BoundingBox(1d, 4d, 1d, 4d);
		
		final BoundingBox boundingBox3 = new BoundingBox(1d, 4d, 1d, 4d, 1d, 4d);
		final BoundingBox boundingBox4 = new BoundingBox(-1d, 2d, -1d, 2d, -1d, 2d);
		
		// Wrong dimensions
		final BoundingBox boundingBoxResult5 = BoundingBox.getCoveringBox(boundingBox1, boundingBox2, boundingBox3, boundingBox4);
		Assert.assertTrue(boundingBoxResult5 == null);
	}

	/**
	 * Test merge on array
	 */
	@Test
	public void testMergeBoxes0() {
		final BoundingBox resultBox = BoundingBox.getCoveringBox();
		Assert.assertEquals(BoundingBox.EMPTY_BOX, resultBox);
	}
	
	/**
	 * Test merge on array
	 */
	@Test
	public void testMergeBoxes1() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 1d);
		final BoundingBox boundingBox2 = new BoundingBox(1d, 1.1d, 1d, 4d);
		final BoundingBox resultBox = BoundingBox.getCoveringBox(boundingBox1, boundingBox2);
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 4f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test merge on array
	 */
	@Test
	public void testMergeBoxes2() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 1d);
		final BoundingBox boundingBox2 = new BoundingBox(1d, 1.1d, 1d, 4d);
		final BoundingBox resultBox = BoundingBox.getCoveringBox(boundingBox1, boundingBox2, null);
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 4f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test merge on array
	 */
	@Test
	public void testMergeBoxes3() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 1d);
		final BoundingBox boundingBox2 = new BoundingBox(1d, 1.1d, 1d, 4d);
		final BoundingBox resultBox = BoundingBox.getCoveringBox(boundingBox1, boundingBox2, BoundingBox.EMPTY_BOX);
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 4f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test merge on array
	 */
	@Test
	public void testMergeBoxes4() {
		final BoundingBox resultBox1 = BoundingBox.getCoveringBox();
		Assert.assertEquals(BoundingBox.EMPTY_BOX, resultBox1);

		final BoundingBox resultBox2 = BoundingBox.getCoveringBox(BoundingBox.EMPTY_BOX);
		Assert.assertEquals(BoundingBox.EMPTY_BOX, resultBox2);

		final BoundingBox resultBox3 = BoundingBox.getCoveringBox(BoundingBox.EMPTY_BOX, BoundingBox.EMPTY_BOX);
		Assert.assertEquals(BoundingBox.EMPTY_BOX, resultBox3);
	}
	
	/**
	 * Test the comparable interface of the bounding box
	 */
	@Test
	public void testBoundingBoxSorting() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 3d, 7d);
		final BoundingBox boundingBox2 = new BoundingBox(-1d, 1d, 3d, 7d);
		final BoundingBox boundingBox3 = new BoundingBox(5d, 7d, 3d, 7d);
		final BoundingBox boundingBox4 = new BoundingBox(-11d, -9d, 3d, 7d);
		final BoundingBox boundingBox5 = new BoundingBox(-11d, -9d, -1d, 3d);
		
		final List<BoundingBox> boundingBoxList = new ArrayList<BoundingBox>();
		boundingBoxList.add(boundingBox1);
		boundingBoxList.add(boundingBox2);
		boundingBoxList.add(boundingBox3);
		boundingBoxList.add(boundingBox4);
		boundingBoxList.add(boundingBox5);
		
		Collections.sort(boundingBoxList);
		
		Assert.assertEquals(boundingBox5, boundingBoxList.get(0));
		Assert.assertEquals(boundingBox4, boundingBoxList.get(1));
		Assert.assertEquals(boundingBox2, boundingBoxList.get(2));
		Assert.assertEquals(boundingBox1, boundingBoxList.get(3));
		Assert.assertEquals(boundingBox3, boundingBoxList.get(4));
	}
	
	/**
	 * Test the split method
	 */
	@Test
	public void testBoundingBoxSplit1() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 3d, 7d);
		final BoundingBox resultBox = boundingBox1.splitAndGetLeft(2d, 0, true);
		Assert.assertArrayEquals(new double[] {1d, 2d, 3d, 7f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test the split method
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxSplit2() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 3d, 7d);
		boundingBox1.splitAndGetLeft(4d, 0, true);
	}
	
	/**
	 * Test the split method  (invalid dimension)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxSplit3() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 3d, 3d, 7d);
		boundingBox1.splitAndGetLeft(1d, 2, true);
	}
	
	/**
	 * Test the open / closed interval split
	 */
	@Test
	public void testBoundingBoxSplit4() {
		final BoundingBox boundingBox = new BoundingBox(1d, 3d, 3d, 7d);
		final BoundingBox leftBox = boundingBox.splitAndGetLeft(2, 0, false);
		final BoundingBox rightBox = boundingBox.splitAndGetRight(2, 0, false);
		
		Assert.assertFalse(leftBox.isCoveringPointInDimension(2, 0));
		Assert.assertFalse(rightBox.isCoveringPointInDimension(2, 0));
	}
	
	/**
	 * Test the bounding box split
	 */
	@Test
	public void testBoundingBoxSplit5() {
		final BoundingBox boundingBox = BoundingBox.createFullCoveringDimensionBoundingBox(1);		
		Assert.assertTrue(boundingBox.isCoveringPointInDimension(0, 0));
		final BoundingBox boundingBoxLeft = boundingBox.splitAndGetLeft(0, 0, false);
		final BoundingBox boundingBoxRight = boundingBox.splitAndGetRight(0, 0, false);
		Assert.assertFalse(boundingBoxLeft.overlaps(boundingBoxRight));
		Assert.assertTrue(boundingBox.overlaps(boundingBoxLeft));
		Assert.assertTrue(boundingBox.overlaps(boundingBoxRight));
		
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test
	public void testIntersection1() {
		final BoundingBox boundingBox = new BoundingBox(1d, 3d, 3d, 7d);
		Assert.assertEquals(BoundingBox.EMPTY_BOX, boundingBox.getIntersection(BoundingBox.EMPTY_BOX));
		Assert.assertEquals(BoundingBox.EMPTY_BOX, BoundingBox.EMPTY_BOX.getIntersection(boundingBox));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test
	public void testIntersection2() {
		final BoundingBox boundingBox = new BoundingBox(1d, 3d, 3d, 7d);		
		Assert.assertEquals(boundingBox, boundingBox.getIntersection(boundingBox));
	}
	
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test
	public void testIntersection3() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 5d, 1d, 5d);	
		final BoundingBox boundingBox2 = new BoundingBox(2d, 4d, 2d, 4d);		

		Assert.assertEquals(boundingBox2, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(boundingBox2, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test
	public void testIntersection4() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 5d, 1d, 5d);	
		final BoundingBox boundingBox2 = new BoundingBox(2d, 6d, 2d, 6d);	
		
		final BoundingBox boundingBoxResult = new BoundingBox(2d, 5d, 2d, 5d);		

		Assert.assertEquals(boundingBoxResult, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(boundingBoxResult, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test
	public void testIntersection5() {
		final BoundingBox boundingBox1 = new BoundingBox(1d, 2d, 1d, 5d);	
		final BoundingBox boundingBox2 = new BoundingBox(6d, 9d, 6d, 9d);	
		
		Assert.assertEquals(BoundingBox.EMPTY_BOX, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(BoundingBox.EMPTY_BOX, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test
	public void testIntersection6() {
		final BoundingBox boundingBox1 = new BoundingBox(1.0d, 2.0d, 1.0d, 2.0d);
		final BoundingBox boundingBox2 = new BoundingBox(1.0d, 3.0d, 1.0d, 3.0d);
		
		Assert.assertEquals(boundingBox1, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(boundingBox1, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the is covered method
	 */
	@Test
	public void testIsFullyCovered1() {
		final BoundingBox boundingBox1 = new BoundingBox(0d, 10d, 0d, 10d);	
		final BoundingBox boundingBox2 = new BoundingBox(1d, 5d, 1d, 5d);	
		final BoundingBox boundingBox3 = new BoundingBox(1d, 11d, 1d, 11d);	

		Assert.assertTrue(boundingBox1.isCovering(boundingBox1));
		Assert.assertTrue(boundingBox1.isCovering(boundingBox2));
		Assert.assertFalse(boundingBox1.isCovering(boundingBox3));
		
		Assert.assertFalse(boundingBox2.isCovering(boundingBox1));
		Assert.assertTrue(boundingBox2.isCovering(boundingBox2));
		Assert.assertFalse(boundingBox2.isCovering(boundingBox3));
		
		Assert.assertFalse(boundingBox3.isCovering(boundingBox1));
		Assert.assertTrue(boundingBox3.isCovering(boundingBox2));
		Assert.assertTrue(boundingBox3.isCovering(boundingBox3));
	}

	/**
	 * Test the is covered method
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testIsFullyCovered2() {
		final BoundingBox boundingBox1 = new BoundingBox(0d, 10d, 0d, 10d);	
		final BoundingBox boundingBox2 = new BoundingBox(0d, 10d, 0d, 10d, 0d, 10d);	
		
		Assert.assertFalse(boundingBox1.isCovering(boundingBox2));
	}
	
	/**
	 * Test the is covered method
	 */
	@Test
	public void testIsFullyCovered3() {
		final BoundingBox boundingBox1 = new BoundingBox(0d, 10d, 0d, 10d);	
		Assert.assertTrue(boundingBox1.isCovering(BoundingBox.EMPTY_BOX));
	}
	
	/**
	 * Test the volume of the bounding box
	 */
	@Test
	public void testVolume() {
		final BoundingBox boundingBox1 = new BoundingBox(0d, 1d, 0d, 1d);	
		final BoundingBox boundingBox2 = new BoundingBox(0d, 10d, 0d, 10d);	
		final BoundingBox boundingBox3 = new BoundingBox(-5d, 5d, -5d, 5d);	

		Assert.assertEquals(1.0, boundingBox1.getVolume(), EQUALS_DELTA);
		Assert.assertEquals(100.0, boundingBox2.getVolume(), EQUALS_DELTA);
		Assert.assertEquals(100.0, boundingBox3.getVolume(), EQUALS_DELTA);
	}
	
	/**
	 * Test the volume of the bounding box
	 */
	@Test
	public void testEnlargement() {
		final BoundingBox boundingBox1 = new BoundingBox(0d, 1d, 0d, 1d);	
		final BoundingBox boundingBox2 = new BoundingBox(0d, 10d, 0d, 10d);	
		final BoundingBox boundingBox3 = new BoundingBox(-5d, 5d, -5d, 5d);	

		Assert.assertEquals(99.0, boundingBox1.calculateEnlargement(boundingBox2), EQUALS_DELTA);
		Assert.assertEquals(99.0, boundingBox1.calculateEnlargement(boundingBox3), EQUALS_DELTA);
		Assert.assertEquals(125.0, boundingBox2.calculateEnlargement(boundingBox3), EQUALS_DELTA);

	}
}
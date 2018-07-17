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
package org.bboxdb.math;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.DoubleInterval;
import org.junit.Assert;
import org.junit.Test;

public class TestHyperrectangle {
	
	protected final static float EQUALS_DELTA = 0.001f;

	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid1() {
		final Hyperrectangle bb1 = new Hyperrectangle(1d, 2d, 3d, 4d, 5d);
	}
	
	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid2() {
		// Dimension 1 error
		final Hyperrectangle bb2 = new Hyperrectangle(2d, -2d, 3d, 4d);
	}
	
	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid3() {	
		// Dimension 2 error
		final Hyperrectangle bb3 = new Hyperrectangle(1d, 2d, 3d, -4d);
	}
	
	/**
	 * Create some valid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(timeout=60000)
	public void testBoundingBoxCreateValid() {
		final Hyperrectangle bb1 = new Hyperrectangle(1d, 10d);
		final Hyperrectangle bb2 = new Hyperrectangle(-10d, 10d);
		final Hyperrectangle bb3 = new Hyperrectangle(1d, 20d, -50d, 50d);
		final Hyperrectangle bb4 = new Hyperrectangle(1d, 20d, -50d, 50d, -100d, 10d);
	}
	
	/**
	 * Test the getter and the setter for the dimension data
	 */
	@Test(timeout=60000)
	public void testGetValues() {
		final Hyperrectangle bb1 = new Hyperrectangle(1d, 11d);
		Assert.assertEquals(1d, bb1.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(10d, bb1.getExtent(0), EQUALS_DELTA);
		
		final Hyperrectangle bb2 = new Hyperrectangle(1d, 21d, -50d, 0d, -100d, -90d);
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
	@Test(timeout=60000)
	public void testLowHigh() {
		final Hyperrectangle bb1 = new Hyperrectangle(1d, 11d);
		Assert.assertEquals(1d, bb1.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(11d, bb1.getCoordinateHigh(0), EQUALS_DELTA);

		final Hyperrectangle bb2 = new Hyperrectangle(1d, 11d, 10d, 60d);
		Assert.assertEquals(1d, bb2.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(11d, bb2.getCoordinateHigh(0), EQUALS_DELTA);
		Assert.assertEquals(10d, bb2.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(60d, bb2.getCoordinateHigh(1), EQUALS_DELTA);
	}
	
	/**
	 * Test the dimension of the bounding box
	 */
	@Test(timeout=60000)
	public void testDimension() {
		final Hyperrectangle bb1 = new Hyperrectangle();
		Assert.assertEquals(0, bb1.getDimension());
		
		final Hyperrectangle bb2 = new Hyperrectangle(1d, 10d);
		Assert.assertEquals(1, bb2.getDimension());
		
		final Hyperrectangle bb3 = new Hyperrectangle(1d, 10d, 10d, 50d);
		Assert.assertEquals(2, bb3.getDimension());
		
		final Hyperrectangle bb4 = new Hyperrectangle(1d, 10d, 10d, 50d, 10d, 10d);
		Assert.assertEquals(3, bb4.getDimension());
	}
	
	/**
	 * Test the overlapping in 1d
	 */
	@Test(timeout=60000)
	public void testOverlapping1D() {
		final Hyperrectangle bb1left = new Hyperrectangle(0d, 10d);
		final Hyperrectangle bb1middle = new Hyperrectangle(5d, 15d);
		final Hyperrectangle bb1right = new Hyperrectangle(10.1d, 20.1d);
		
		Assert.assertTrue(bb1left.intersects(bb1left));
		Assert.assertTrue(bb1middle.intersects(bb1middle));
		Assert.assertTrue(bb1right.intersects(bb1right));
		
		Assert.assertFalse(bb1left.intersects(null));
		Assert.assertFalse(bb1middle.intersects(null));
		Assert.assertFalse(bb1right.intersects(null));
		
		Assert.assertFalse(bb1left.intersects(bb1right));
		Assert.assertFalse(bb1right.intersects(bb1left));

		Assert.assertTrue(bb1left.intersects(bb1middle));
		Assert.assertTrue(bb1middle.intersects(bb1left));

		Assert.assertTrue(bb1middle.intersects(bb1right));
		Assert.assertTrue(bb1right.intersects(bb1middle));		
	}
	
	/**
	 * Test the overlapping in 2d
	 */
	@Test(timeout=60000)
	public void testOverlapping2D() {
		final Hyperrectangle bb1left = new Hyperrectangle(0d, 1d, 0d, 1d);
		final Hyperrectangle bb1leftinside = new Hyperrectangle(0.5d, 0.7d, 0.5d, 0.7d);
		
		Assert.assertTrue(bb1left.intersects(bb1leftinside));
		Assert.assertTrue(bb1leftinside.intersects(bb1left));

		final Hyperrectangle bb1middle = new Hyperrectangle(0.5d, 1.5d, 0.5d, 1.5d);
		Assert.assertTrue(bb1left.intersects(bb1middle));
		Assert.assertTrue(bb1middle.intersects(bb1left));

		final Hyperrectangle bb1right = new Hyperrectangle(1d, 2d, 10d, 11d);
		Assert.assertFalse(bb1left.intersects(bb1right));
		Assert.assertFalse(bb1right.intersects(bb1left));
	}
	
	/**
	 * Test the overlapping in 3d
	 */
	@Test(timeout=60000)
	public void testOverlapping3D() {
		final Hyperrectangle bb1left = new Hyperrectangle(0d, 1d, 0d, 1d, 0d, 1d);
		final Hyperrectangle bb1leftinside = new Hyperrectangle(0.5d, 0.7d, 0.5d, 0.7d, 0.5d, 0.7d);
		Assert.assertTrue(bb1left.intersects(bb1leftinside));
		Assert.assertTrue(bb1leftinside.intersects(bb1left));
		
		final Hyperrectangle bb1middle = new Hyperrectangle(0.5d, 1.5d, 0.5d, 1.5d, 0.5d, 1.5d);
		Assert.assertTrue(bb1left.intersects(bb1middle));
		Assert.assertTrue(bb1middle.intersects(bb1left));

		final Hyperrectangle bb1right = new Hyperrectangle(10d, 11d, 10d, 11d, 10d, 11d);
		Assert.assertFalse(bb1left.intersects(bb1right));
		Assert.assertFalse(bb1right.intersects(bb1left));
	}
	
	/**
	 * Test empty bounding box overlapping
	 */
	@Test(timeout=60000)
	public void testOverlapEmptyBoundingBox() {
		final Hyperrectangle bb1left = new Hyperrectangle(0d, 1d, 0d, 1d, 0d, 1d);
		Assert.assertTrue(bb1left.intersects(Hyperrectangle.FULL_SPACE));
		Assert.assertTrue(Hyperrectangle.FULL_SPACE.intersects(Hyperrectangle.FULL_SPACE));
	}
	
	/**
	 * Test the creation of the covering bounding box 
	 */
	@Test(timeout=60000)
	public void testCoverBoundingBox1() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 1d, 3d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 4d, 1d, 4d);
		
		final Hyperrectangle boundingBox3 = new Hyperrectangle(1d, 4d, 1d, 4d, 1d, 4d);
		final Hyperrectangle boundingBox4 = new Hyperrectangle(-1d, 2d, -1d, 2d, -1d, 2d);

		final Hyperrectangle boundingBoxResult1 = Hyperrectangle.getCoveringBox();
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, boundingBoxResult1);
		
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(boundingBox1));
		Assert.assertEquals(boundingBox2, Hyperrectangle.getCoveringBox(boundingBox2));
		Assert.assertEquals(boundingBox3, Hyperrectangle.getCoveringBox(boundingBox3));
		Assert.assertEquals(boundingBox4, Hyperrectangle.getCoveringBox(boundingBox4));
		
		final Hyperrectangle boundingBoxResult2 = Hyperrectangle.getCoveringBox(boundingBox1, boundingBox2);
		Assert.assertEquals(2, boundingBoxResult2.getDimension());
		Assert.assertEquals(boundingBoxResult2, boundingBox2);

		final Hyperrectangle boundingBoxResult4 = Hyperrectangle.getCoveringBox(boundingBox3, boundingBox4);
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
	@Test(timeout=60000)
	public void testCoverBoundingBox2() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 1d, 3d);
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(boundingBox1));
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(boundingBox1, null));
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(null, boundingBox1));
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(boundingBox1, Hyperrectangle.FULL_SPACE));
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(Hyperrectangle.FULL_SPACE, boundingBox1, Hyperrectangle.FULL_SPACE));
		Assert.assertEquals(boundingBox1, Hyperrectangle.getCoveringBox(Hyperrectangle.FULL_SPACE, null, boundingBox1, Hyperrectangle.FULL_SPACE));
	}
	
	/**
	 * Merge two boxes with wrong dimension
	 */
	@Test(expected=IllegalArgumentException.class)
	public void mergeBoxesWithWrongDimension1() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 1d, 3d);		
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 4d, 1d, 4d, 1d, 4d);
		
		final Hyperrectangle boundingBoxResult3 = Hyperrectangle.getCoveringBox(boundingBox1, boundingBox2);
		Assert.assertTrue(boundingBoxResult3 == null);
	}
	
	/**
	 * Merge two boxes with wrong dimension
	 */
	@Test(expected=IllegalArgumentException.class)
	public void mergeBoxesWithWrongDimension2() {
		
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 1d, 3d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 4d, 1d, 4d);
		
		final Hyperrectangle boundingBox3 = new Hyperrectangle(1d, 4d, 1d, 4d, 1d, 4d);
		final Hyperrectangle boundingBox4 = new Hyperrectangle(-1d, 2d, -1d, 2d, -1d, 2d);
		
		// Wrong dimensions
		final Hyperrectangle boundingBoxResult5 = Hyperrectangle.getCoveringBox(boundingBox1, boundingBox2, boundingBox3, boundingBox4);
		Assert.assertTrue(boundingBoxResult5 == null);
	}

	/**
	 * Test merge on array
	 */
	@Test(timeout=60000)
	public void testMergeBoxes0() {
		final Hyperrectangle resultBox = Hyperrectangle.getCoveringBox();
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, resultBox);
	}
	
	/**
	 * Test merge on array
	 */
	@Test(timeout=60000)
	public void testMergeBoxes1() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 2d, 1d, 1d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 1.1d, 1d, 4d);
		final Hyperrectangle resultBox = Hyperrectangle.getCoveringBox(boundingBox1, boundingBox2);
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 4f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test merge on array
	 */
	@Test(timeout=60000)
	public void testMergeBoxes2() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 2d, 1d, 1d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 1.1d, 1d, 4d);
		final Hyperrectangle resultBox = Hyperrectangle.getCoveringBox(boundingBox1, boundingBox2, null);
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 4f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test merge on array
	 */
	@Test(timeout=60000)
	public void testMergeBoxes3() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 2d, 1d, 1d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 1.1d, 1d, 4d);
		final Hyperrectangle resultBox = Hyperrectangle.getCoveringBox(boundingBox1, boundingBox2, Hyperrectangle.FULL_SPACE);
		Assert.assertArrayEquals(new double[] {1d, 2d, 1d, 4f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test merge on array
	 */
	@Test(timeout=60000)
	public void testMergeBoxes4() {
		final Hyperrectangle resultBox1 = Hyperrectangle.getCoveringBox();
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, resultBox1);

		final Hyperrectangle resultBox2 = Hyperrectangle.getCoveringBox(Hyperrectangle.FULL_SPACE);
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, resultBox2);

		final Hyperrectangle resultBox3 = Hyperrectangle.getCoveringBox(Hyperrectangle.FULL_SPACE, Hyperrectangle.FULL_SPACE);
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, resultBox3);
	}
	
	/**
	 * Test the comparable interface of the bounding box
	 */
	@Test(timeout=60000)
	public void testBoundingBoxSorting() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 3d, 7d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(-1d, 1d, 3d, 7d);
		final Hyperrectangle boundingBox3 = new Hyperrectangle(5d, 7d, 3d, 7d);
		final Hyperrectangle boundingBox4 = new Hyperrectangle(-11d, -9d, 3d, 7d);
		final Hyperrectangle boundingBox5 = new Hyperrectangle(-11d, -9d, -1d, 3d);
		
		final List<Hyperrectangle> boundingBoxList = new ArrayList<Hyperrectangle>();
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
	@Test(timeout=60000)
	public void testBoundingBoxSplit1() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 3d, 7d);
		final Hyperrectangle resultBox = boundingBox1.splitAndGetLeft(2d, 0, true);
		Assert.assertArrayEquals(new double[] {1d, 2d, 3d, 7f}, resultBox.toDoubleArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test the split method
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxSplit2() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 3d, 7d);
		boundingBox1.splitAndGetLeft(4d, 0, true);
	}
	
	/**
	 * Test the split method  (invalid dimension)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxSplit3() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 3d, 3d, 7d);
		boundingBox1.splitAndGetLeft(1d, 2, true);
	}
	
	/**
	 * Test the open / closed interval split
	 */
	@Test(timeout=60000)
	public void testBoundingBoxSplit4() {
		final Hyperrectangle boundingBox = new Hyperrectangle(1d, 3d, 3d, 7d);
		final Hyperrectangle leftBox = boundingBox.splitAndGetLeft(2, 0, false);
		final Hyperrectangle rightBox = boundingBox.splitAndGetRight(2, 0, false);
		
		Assert.assertFalse(leftBox.isCoveringPointInDimension(2, 0));
		Assert.assertFalse(rightBox.isCoveringPointInDimension(2, 0));
	}
	
	/**
	 * Test the bounding box split
	 */
	@Test(timeout=60000)
	public void testBoundingBoxSplit5() {
		final Hyperrectangle boundingBox = Hyperrectangle.createFullCoveringDimensionBoundingBox(1);		
		Assert.assertTrue(boundingBox.isCoveringPointInDimension(0, 0));
		final Hyperrectangle boundingBoxLeft = boundingBox.splitAndGetLeft(0, 0, false);
		final Hyperrectangle boundingBoxRight = boundingBox.splitAndGetRight(0, 0, false);
		Assert.assertFalse(boundingBoxLeft.intersects(boundingBoxRight));
		Assert.assertTrue(boundingBox.intersects(boundingBoxLeft));
		Assert.assertTrue(boundingBox.intersects(boundingBoxRight));
		
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test(timeout=60000)
	public void testIntersection1() {
		final Hyperrectangle boundingBox = new Hyperrectangle(1d, 3d, 3d, 7d);
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, boundingBox.getIntersection(Hyperrectangle.FULL_SPACE));
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, Hyperrectangle.FULL_SPACE.getIntersection(boundingBox));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test(timeout=60000)
	public void testIntersection2() {
		final Hyperrectangle boundingBox = new Hyperrectangle(1d, 3d, 3d, 7d);		
		Assert.assertEquals(boundingBox, boundingBox.getIntersection(boundingBox));
	}
	
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test(timeout=60000)
	public void testIntersection3() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 5d, 1d, 5d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(2d, 4d, 2d, 4d);		

		Assert.assertEquals(boundingBox2, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(boundingBox2, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test(timeout=60000)
	public void testIntersection4() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 5d, 1d, 5d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(2d, 6d, 2d, 6d);	
		
		final Hyperrectangle boundingBoxResult = new Hyperrectangle(2d, 5d, 2d, 5d);		

		Assert.assertEquals(boundingBoxResult, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(boundingBoxResult, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test(timeout=60000)
	public void testIntersection5() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 2d, 1d, 5d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(6d, 9d, 6d, 9d);	
		
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(Hyperrectangle.FULL_SPACE, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the intersection of two bounding boxes
	 */
	@Test(timeout=60000)
	public void testIntersection6() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1.0d, 2.0d, 1.0d, 2.0d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1.0d, 3.0d, 1.0d, 3.0d);
		
		Assert.assertEquals(boundingBox1, boundingBox1.getIntersection(boundingBox2));
		Assert.assertEquals(boundingBox1, boundingBox2.getIntersection(boundingBox1));
	}
	
	/**
	 * Test the is covered method
	 */
	@Test(timeout=60000)
	public void testIsFullyCovered1() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(0d, 10d, 0d, 10d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 5d, 1d, 5d);	
		final Hyperrectangle boundingBox3 = new Hyperrectangle(1d, 11d, 1d, 11d);	

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
		final Hyperrectangle boundingBox1 = new Hyperrectangle(0d, 10d, 0d, 10d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(0d, 10d, 0d, 10d, 0d, 10d);	
		
		Assert.assertFalse(boundingBox1.isCovering(boundingBox2));
	}
	
	/**
	 * Test the is covered method
	 */
	@Test(timeout=60000)
	public void testIsFullyCovered3() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(0d, 10d, 0d, 10d);	
		Assert.assertTrue(boundingBox1.isCovering(Hyperrectangle.FULL_SPACE));
	}
	
	/**
	 * Test the volume of the bounding box
	 */
	@Test(timeout=60000)
	public void testVolume() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(0d, 1d, 0d, 1d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(0d, 10d, 0d, 10d);	
		final Hyperrectangle boundingBox3 = new Hyperrectangle(-5d, 5d, -5d, 5d);	

		Assert.assertEquals(1.0, boundingBox1.getVolume(), EQUALS_DELTA);
		Assert.assertEquals(100.0, boundingBox2.getVolume(), EQUALS_DELTA);
		Assert.assertEquals(100.0, boundingBox3.getVolume(), EQUALS_DELTA);
	}
	
	/**
	 * Test the volume of the bounding box
	 */
	@Test(timeout=60000)
	public void testEnlargement() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(0d, 1d, 0d, 1d);	
		final Hyperrectangle boundingBox2 = new Hyperrectangle(0d, 10d, 0d, 10d);	
		final Hyperrectangle boundingBox3 = new Hyperrectangle(-5d, 5d, -5d, 5d);	

		Assert.assertEquals(99.0, boundingBox1.calculateEnlargement(boundingBox2), EQUALS_DELTA);
		Assert.assertEquals(99.0, boundingBox1.calculateEnlargement(boundingBox3), EQUALS_DELTA);
		Assert.assertEquals(125.0, boundingBox2.calculateEnlargement(boundingBox3), EQUALS_DELTA);
	}
	
	/**
	 * Test the from and to string method
	 */
	@Test(timeout=60000)
	public void testFromToString1() {
		final Hyperrectangle boundingBox1 = Hyperrectangle.FULL_SPACE;
		final Hyperrectangle boundingBox2 = new Hyperrectangle(0d, 1d);	
		final Hyperrectangle boundingBox3 = new Hyperrectangle(-5d, 5d, -5d, 5d);	
		final Hyperrectangle boundingBox4 = new Hyperrectangle(Arrays.asList(new DoubleInterval(1, 2, false, false)));	
		final Hyperrectangle boundingBox5 = new Hyperrectangle(Arrays.asList(new DoubleInterval(1, 2, false, true)));	
		final Hyperrectangle boundingBox6 = new Hyperrectangle(Arrays.asList(new DoubleInterval(1, 2, true, false)));	
		final Hyperrectangle boundingBox7 = new Hyperrectangle(Arrays.asList(new DoubleInterval(1, 2, true, false), new DoubleInterval(1, 2, false, false)));	
		final Hyperrectangle boundingBox8 = Hyperrectangle.createFullCoveringDimensionBoundingBox(3);
		
		Assert.assertEquals(boundingBox1, new Hyperrectangle(boundingBox1.toCompactString()));
		Assert.assertEquals(boundingBox2, new Hyperrectangle(boundingBox2.toCompactString()));
		Assert.assertEquals(boundingBox3, new Hyperrectangle(boundingBox3.toCompactString()));
		Assert.assertEquals(boundingBox4, new Hyperrectangle(boundingBox4.toCompactString()));
		Assert.assertEquals(boundingBox5, new Hyperrectangle(boundingBox5.toCompactString()));
		Assert.assertEquals(boundingBox6, new Hyperrectangle(boundingBox6.toCompactString()));
		Assert.assertEquals(boundingBox7, new Hyperrectangle(boundingBox7.toCompactString()));
		Assert.assertEquals(boundingBox8, new Hyperrectangle(boundingBox8.toCompactString()));
	}
	
	/**
	 * Test the from and to string method
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testFromToString2() {
		new Hyperrectangle("sdsfsd");
	}
	
	/**
	 * Test the to string method
	 */
	@Test(timeout=60000)
	public void testToString() {
		final Hyperrectangle boundingBox = new Hyperrectangle(Arrays.asList(new DoubleInterval(1, 2, true, false)));	
		Assert.assertTrue(boundingBox.toString().length() > 10);
		boundingBox.hashCode();
	}
	
	/**
	 * Test the compare to method
	 */
	@Test(timeout=60000)
	public void testCompareTo() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 2d, 3d, 4d);
		final Hyperrectangle boundingBox2 = new Hyperrectangle(1d, 2d);
		
		Assert.assertEquals(0, boundingBox1.compareTo(boundingBox1));
		Assert.assertTrue(boundingBox1.compareTo(boundingBox2) > 0);
		Assert.assertTrue(boundingBox2.compareTo(boundingBox1) < 0);
	}
	
	/**
	 * Test the size
	 */
	@Test(timeout=60000)
	public void testSize() {
		final Hyperrectangle boundingBox = new Hyperrectangle(Arrays.asList(new DoubleInterval(1, 2, true, false)));	
		Assert.assertTrue(boundingBox.getSize() > 0);
	}
	
	/**
	 * Test from and to byte array
	 */
	@Test(timeout=60000)
	public void testFromAndToByteArray() {
		final Hyperrectangle boundingBox1 = new Hyperrectangle(1d, 2d, 3d, 4d);
		final byte[] bytes = boundingBox1.toByteArray();
		final Hyperrectangle boundingBox2 = Hyperrectangle.fromByteArray(bytes);
		Assert.assertEquals(boundingBox1, boundingBox2);
	}
}

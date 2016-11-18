/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;

public class TestBoundingBox {
	
	protected final static float EQUALS_DELTA = 0.001f;

	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid1() {
		final BoundingBox bb1 = new BoundingBox(1f, 2f, 3f, 4f, 5f);
	}
	
	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid2() {
		// Dimension 1 error
		final BoundingBox bb2 = new BoundingBox(2f, -2f, 3f, 4f);
	}
	
	/**
	 * Create some invalid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxCreateInvalid3() {	
		// Dimension 2 error
		final BoundingBox bb3 = new BoundingBox(1f, 2f, 3f, -4f);
	}
	
	/**
	 * Create some valid bounding boxes
	 */
	@SuppressWarnings("unused")
	@Test
	public void testBoundingBoxCreateValid() {
		final BoundingBox bb1 = new BoundingBox(1f, 10f);
		final BoundingBox bb2 = new BoundingBox(-10f, 10f);
		final BoundingBox bb3 = new BoundingBox(1f, 20f, -50f, 50f);
		final BoundingBox bb4 = new BoundingBox(1f, 20f, -50f, 50f, -100f, 10f);
	}
	
	/**
	 * Test the getter and the setter for the dimension data
	 */
	@Test
	public void testGetValues() {
		final BoundingBox bb1 = new BoundingBox(1f, 11f);
		Assert.assertEquals(1f, bb1.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(10f, bb1.getExtent(0), EQUALS_DELTA);
		
		final BoundingBox bb2 = new BoundingBox(1f, 21f, -50f, 0f, -100f, -90f);
		Assert.assertEquals(1f, bb2.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(20f, bb2.getExtent(0), EQUALS_DELTA);
		Assert.assertEquals(-50f, bb2.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(50f, bb2.getExtent(1), EQUALS_DELTA);
		Assert.assertEquals(-100f, bb2.getCoordinateLow(2), EQUALS_DELTA);
		Assert.assertEquals(10f, bb2.getExtent(2), EQUALS_DELTA);
	}
	
	/**
	 * Test the calculation of low and high values
	 */
	@Test
	public void testLowHigh() {
		final BoundingBox bb1 = new BoundingBox(1f, 11f);
		Assert.assertEquals(1f, bb1.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(11f, bb1.getCoordinateHigh(0), EQUALS_DELTA);

		final BoundingBox bb2 = new BoundingBox(1f, 11f, 10f, 60f);
		Assert.assertEquals(1f, bb2.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(11f, bb2.getCoordinateHigh(0), EQUALS_DELTA);
		Assert.assertEquals(10f, bb2.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(60f, bb2.getCoordinateHigh(1), EQUALS_DELTA);
	}
	
	/**
	 * Test the dimension of the bounding box
	 */
	@Test
	public void testDimension() {
		final BoundingBox bb1 = new BoundingBox();
		Assert.assertEquals(0, bb1.getDimension());
		
		final BoundingBox bb2 = new BoundingBox(1f, 10f);
		Assert.assertEquals(1, bb2.getDimension());
		
		final BoundingBox bb3 = new BoundingBox(1f, 10f, 10f, 50f);
		Assert.assertEquals(2, bb3.getDimension());
		
		final BoundingBox bb4 = new BoundingBox(1f, 10f, 10f, 50f, 10f, 10f);
		Assert.assertEquals(3, bb4.getDimension());
	}
	
	/**
	 * Test the overlapping in 1d
	 */
	@Test
	public void testOverlapping1D() {
		final BoundingBox bb1left = new BoundingBox(0f, 10f);
		final BoundingBox bb1middle = new BoundingBox(5f, 15f);
		final BoundingBox bb1right = new BoundingBox(10.1f, 20.1f);
		
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
		final BoundingBox bb1left = new BoundingBox(0f, 1f, 0f, 1f);
		final BoundingBox bb1leftinside = new BoundingBox(0.5f, 0.7f, 0.5f, 0.7f);
		
		Assert.assertTrue(bb1left.overlaps(bb1leftinside));
		Assert.assertTrue(bb1leftinside.overlaps(bb1left));

		final BoundingBox bb1middle = new BoundingBox(0.5f, 1.5f, 0.5f, 1.5f);
		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		final BoundingBox bb1right = new BoundingBox(1f, 2f, 10f, 11f);
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));
	}
	
	/**
	 * Test the overlapping in 3d
	 */
	@Test
	public void testOverlapping3D() {
		final BoundingBox bb1left = new BoundingBox(0f, 1f, 0f, 1f, 0f, 1f);
		final BoundingBox bb1leftinside = new BoundingBox(0.5f, 0.7f, 0.5f, 0.7f, 0.5f, 0.7f);
		Assert.assertTrue(bb1left.overlaps(bb1leftinside));
		Assert.assertTrue(bb1leftinside.overlaps(bb1left));
		
		final BoundingBox bb1middle = new BoundingBox(0.5f, 1.5f, 0.5f, 1.5f, 0.5f, 1.5f);
		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		final BoundingBox bb1right = new BoundingBox(10f, 11f, 10f, 11f, 10f, 11f);
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));
	}
	
	/**
	 * Test empty bounding box overlapping
	 */
	@Test
	public void testOverlapEmptyBoundingBox() {
		final BoundingBox bb1left = new BoundingBox(0f, 1f, 0f, 1f, 0f, 1f);
		Assert.assertTrue(bb1left.overlaps(BoundingBox.EMPTY_BOX));
		Assert.assertTrue(BoundingBox.EMPTY_BOX.overlaps(BoundingBox.EMPTY_BOX));
	}
	
	/**
	 * Test the creation of the covering bounding box 
	 */
	@Test
	public void testCoverBoundingBox() {
		final BoundingBox boundingBox1 = new BoundingBox(1f, 3f, 1f, 3f);
		final BoundingBox boundingBox2 = new BoundingBox(1f, 4f, 1f, 4f);
		
		final BoundingBox boundingBox3 = new BoundingBox(1f, 4f, 1f, 4f, 1f, 4f);
		final BoundingBox boundingBox4 = new BoundingBox(-1f, 2f, -1f, 2f, -1f, 2f);

		final BoundingBox boundingBoxResult1 = BoundingBox.getBoundingBox();
		Assert.assertTrue(boundingBoxResult1 == null);
		
		Assert.assertEquals(boundingBox1, BoundingBox.getBoundingBox(boundingBox1));
		Assert.assertEquals(boundingBox2, BoundingBox.getBoundingBox(boundingBox2));
		Assert.assertEquals(boundingBox3, BoundingBox.getBoundingBox(boundingBox3));
		Assert.assertEquals(boundingBox4, BoundingBox.getBoundingBox(boundingBox4));
		
		final BoundingBox boundingBoxResult2 = BoundingBox.getBoundingBox(boundingBox1, boundingBox2);
		Assert.assertEquals(2, boundingBoxResult2.getDimension());
		Assert.assertEquals(boundingBoxResult2, boundingBox2);
		
		// Wrong dimensions
		final BoundingBox boundingBoxResult3 = BoundingBox.getBoundingBox(boundingBox1, boundingBox3);
		Assert.assertTrue(boundingBoxResult3 == null);

		final BoundingBox boundingBoxResult4 = BoundingBox.getBoundingBox(boundingBox3, boundingBox4);
		Assert.assertEquals(3, boundingBoxResult4.getDimension());		
		Assert.assertEquals(-1.0f, boundingBoxResult4.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(4.0f, boundingBoxResult4.getCoordinateHigh(0), EQUALS_DELTA);
		Assert.assertEquals(-1.0f, boundingBoxResult4.getCoordinateLow(1), EQUALS_DELTA);
		Assert.assertEquals(4.0f, boundingBoxResult4.getCoordinateHigh(1), EQUALS_DELTA);
		Assert.assertEquals(-1.0f, boundingBoxResult4.getCoordinateLow(2), EQUALS_DELTA);
		Assert.assertEquals(4.0f, boundingBoxResult4.getCoordinateHigh(2), EQUALS_DELTA);
		
		// Wrong dimensions
		final BoundingBox boundingBoxResult5 = BoundingBox.getBoundingBox(boundingBox1, boundingBox2, boundingBox3, boundingBox4);
		Assert.assertTrue(boundingBoxResult5 == null);
	}
	
	/**
	 * Test merge on array
	 */
	@Test
	public void testMergeBoxes() {
		final BoundingBox boundingBox1 = new BoundingBox(1f, 2f, 1f, 1f);
		final BoundingBox boundingBox2 = new BoundingBox(1f, 1.1f, 1f, 4f);
		final BoundingBox resultBox = BoundingBox.getBoundingBox(boundingBox1, boundingBox2);
		Assert.assertArrayEquals(new float[] {1f, 2f, 1f, 4f}, resultBox.toFloatArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test the comparable interface of the bounding box
	 */
	@Test
	public void testBoundingBoxSorting() {
		final BoundingBox boundingBox1 = new BoundingBox(1f, 3f, 3f, 7f);
		final BoundingBox boundingBox2 = new BoundingBox(-1f, 1f, 3f, 7f);
		final BoundingBox boundingBox3 = new BoundingBox(5f, 7f, 3f, 7f);
		final BoundingBox boundingBox4 = new BoundingBox(-11f, -9f, 3f, 7f);
		final BoundingBox boundingBox5 = new BoundingBox(-11f, -9f, -1f, 3f);
		
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
		final BoundingBox boundingBox1 = new BoundingBox(1f, 3f, 3f, 7f);
		final BoundingBox resultBox = boundingBox1.splitAndGetLeft(2f, 0, true);
		Assert.assertArrayEquals(new float[] {1f, 2f, 3f, 7f}, resultBox.toFloatArray(), EQUALS_DELTA);
	}
	
	/**
	 * Test the split method
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxSplit2() {
		final BoundingBox boundingBox1 = new BoundingBox(1f, 3f, 3f, 7f);
		boundingBox1.splitAndGetLeft(4f, 0, true);
	}
	
	/**
	 * Test the split method  (invalid dimension)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBoundingBoxSplit3() {
		final BoundingBox boundingBox1 = new BoundingBox(1f, 3f, 3f, 7f);
		boundingBox1.splitAndGetLeft(1f, 2, true);
	}
	
	/**
	 * Test the open / closed interval split
	 */
	@Test
	public void testBoundingBoxSplit4() {
		final BoundingBox boundingBox = new BoundingBox(1f, 3f, 3f, 7f);
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

}

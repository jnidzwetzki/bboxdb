package de.fernunihagen.dna.jkn.scalephant;

import junit.framework.Assert;

import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;

public class TestBoundingBox {

	/**
	 * Create some invalid bounding boxes
	 */
	@Test
	public void testBoundingBoxCreateInvalid() {
		final BoundingBox bb1 = new BoundingBox(1f, 2f, 3f, 4f, 5f);
		Assert.assertFalse(bb1.isValid());
		
		// Dimension 1 error
		final BoundingBox bb2 = new BoundingBox(2f, -2f, 3f, 4f);
		Assert.assertFalse(bb2.isValid());
		
		// Dimension 2 error
		final BoundingBox bb3 = new BoundingBox(1f, 2f, 3f, -4f);
		Assert.assertFalse(bb3.isValid());
	}
	
	/**
	 * Create some valid bounding boxes
	 */
	@Test
	public void testBoundingBoxCreateValid() {
		final BoundingBox bb1 = new BoundingBox(1f, 10f);
		Assert.assertTrue(bb1.isValid());

		final BoundingBox bb2 = new BoundingBox(-10f, 10f);
		Assert.assertTrue(bb2.isValid());

		final BoundingBox bb3 = new BoundingBox(1f, 20f, -50f, 50f);
		Assert.assertTrue(bb3.isValid());

		final BoundingBox bb4 = new BoundingBox(1f, 20f, -50f, 50f, -100f, 10f);
		Assert.assertTrue(bb4.isValid());
	}
	
	/**
	 * Test the getter and the setter for the dimension data
	 */
	@Test
	public void testGetValues() {
		final BoundingBox bb1 = new BoundingBox(1f, 10f);
		Assert.assertEquals(1f, bb1.getCoordinateLow(0));
		Assert.assertEquals(10f, bb1.getExtent(0));
		
		final BoundingBox bb2 = new BoundingBox(1f, 20f, -50f, 50f, -100f, 10f);
		Assert.assertEquals(1f, bb2.getCoordinateLow(0));
		Assert.assertEquals(20f, bb2.getExtent(0));
		Assert.assertEquals(-50f, bb2.getCoordinateLow(1));
		Assert.assertEquals(50f, bb2.getExtent(1));
		Assert.assertEquals(-100f, bb2.getCoordinateLow(2));
		Assert.assertEquals(10f, bb2.getExtent(2));
	}
	
	/**
	 * Test the calculation of low and high values
	 */
	@Test
	public void testLowHigh() {
		final BoundingBox bb1 = new BoundingBox(1f, 10f);
		Assert.assertEquals(1f, bb1.getCoordinateLow(0));
		Assert.assertEquals(11f, bb1.getCoordinateHigh(0));

		final BoundingBox bb2 = new BoundingBox(1f, 10f, 10f, 50f);
		Assert.assertEquals(1f, bb2.getCoordinateLow(0));
		Assert.assertEquals(11f, bb2.getCoordinateHigh(0));
		Assert.assertEquals(10f, bb2.getCoordinateLow(1));
		Assert.assertEquals(60f, bb2.getCoordinateHigh(1));
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
		
		final BoundingBox bb5 = new BoundingBox(1f, 10f, 10f);
		Assert.assertEquals(BoundingBox.INVALID_DIMENSION, bb5.getDimension());
	}
	
	/**
	 * Test the overlapping in 1d
	 */
	@Test
	public void testOverlapping1D() {
		final BoundingBox bb1left = new BoundingBox(0f, 10f);
		final BoundingBox bb1middle = new BoundingBox(5f, 10f);
		final BoundingBox bb1right = new BoundingBox(10.1f, 10f);
		
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
		final BoundingBox bb1leftinside = new BoundingBox(0.5f, 0.2f, 0.5f, 0.2f);
		
		System.out.println(bb1left);
		System.out.println(bb1leftinside);
		
		Assert.assertTrue(bb1left.overlaps(bb1leftinside));
		Assert.assertTrue(bb1leftinside.overlaps(bb1left));

		final BoundingBox bb1middle = new BoundingBox(0.5f, 1f, 0.5f, 1f);
		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		final BoundingBox bb1right = new BoundingBox(1f, 1f, 10f, 1f);
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));
	}
	
	/**
	 * Test the overlapping in 3d
	 */
	@Test
	public void testOverlapping3D() {
		final BoundingBox bb1left = new BoundingBox(0f, 1f, 0f, 1f, 0f, 1f);
		final BoundingBox bb1leftinside = new BoundingBox(0.5f, 0.2f, 0.5f, 0.2f, 0.5f, 0.2f);
		Assert.assertTrue(bb1left.overlaps(bb1leftinside));
		Assert.assertTrue(bb1leftinside.overlaps(bb1left));
		
		final BoundingBox bb1middle = new BoundingBox(0.5f, 1f, 0.5f, 1f, 0.5f, 1f);
		Assert.assertTrue(bb1left.overlaps(bb1middle));
		Assert.assertTrue(bb1middle.overlaps(bb1left));

		final BoundingBox bb1right = new BoundingBox(10f, 1f, 10f, 1f, 10f, 1f);
		Assert.assertFalse(bb1left.overlaps(bb1right));
		Assert.assertFalse(bb1right.overlaps(bb1left));
	}
	
}

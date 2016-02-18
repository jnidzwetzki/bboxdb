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
	
}

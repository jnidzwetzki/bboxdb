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
	
}

package de.fernunihagen.dna.jkn.scalephant.storage;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.FloatInterval;

public class TestFloatInterval {

	/**
	 * Create an invalid interval
	 */
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void invalidInterval1() {
		final FloatInterval floatInterval = new FloatInterval(-5, -10);
	}

	/**
	 * Create an invalid interval
	 */
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void invalidInterval2() {
		final FloatInterval floatInterval = new FloatInterval(-5, -5, false, false);
	}
	
	/**
	 * Test cover method
	 */
	@Test
	public void coveredPoint1() {
		final FloatInterval floatInterval = new FloatInterval(1, 100);
		Assert.assertTrue(floatInterval.isBeginIncluded());
		Assert.assertTrue(floatInterval.isEndIncluded());
		Assert.assertTrue(floatInterval.isNumberCovered(floatInterval.getBegin(), true));
		Assert.assertTrue(floatInterval.isNumberCovered(floatInterval.getEnd(), true));
		Assert.assertFalse(floatInterval.isNumberCovered(floatInterval.getBegin(), false));
		Assert.assertFalse(floatInterval.isNumberCovered(floatInterval.getEnd(), false));
		Assert.assertTrue(floatInterval.isNumberCovered(50, true));
		Assert.assertTrue(floatInterval.isNumberCovered(50, false));

		Assert.assertFalse(floatInterval.isNumberCovered(-10, true));
		Assert.assertFalse(floatInterval.isNumberCovered(110, true));
	}
	
	/**
	 * Test cover method
	 */
	@Test
	public void coveredPoint2() {
		final FloatInterval floatInterval = new FloatInterval(1, 100, false, false);
		Assert.assertFalse(floatInterval.isBeginIncluded());
		Assert.assertFalse(floatInterval.isEndIncluded());
		Assert.assertFalse(floatInterval.isNumberCovered(floatInterval.getBegin(), true));
		Assert.assertFalse(floatInterval.isNumberCovered(floatInterval.getEnd(), true));
		Assert.assertFalse(floatInterval.isNumberCovered(floatInterval.getBegin(), false));
		Assert.assertFalse(floatInterval.isNumberCovered(floatInterval.getEnd(), false));
		Assert.assertTrue(floatInterval.isNumberCovered(50, true));
		Assert.assertTrue(floatInterval.isNumberCovered(50, false));
		
		Assert.assertFalse(floatInterval.isNumberCovered(-10, true));
		Assert.assertFalse(floatInterval.isNumberCovered(110, true));
	}
	
	/**
	 * Test overlap method (closed intervals)
	 */
	@Test
	public void intervalCovered1() {
		final FloatInterval floatInterval1 = new FloatInterval(1, 100);
		final FloatInterval floatInterval2 = new FloatInterval(50, 150);
		final FloatInterval floatInterval3 = new FloatInterval(500, 900);
		final FloatInterval floatInterval4 = new FloatInterval(100, 101);

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval1));
		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval4));
		
		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval1));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval4.isOverlappingWith(floatInterval4));

		Assert.assertFalse(floatInterval1.isOverlappingWith(floatInterval3));
		Assert.assertFalse(floatInterval2.isOverlappingWith(floatInterval3));
	}
	
	/**
	 * Test overlap method (open intervals)
	 */
	@Test
	public void intervalCovered2() {
		final FloatInterval floatInterval1 = new FloatInterval(1, 100, false, false);
		final FloatInterval floatInterval2 = new FloatInterval(50, 150, false, false);
		final FloatInterval floatInterval3 = new FloatInterval(0, 101, false, false);
		final FloatInterval floatInterval4 = new FloatInterval(100, 101, false, false);

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval1));
		Assert.assertFalse(floatInterval1.isOverlappingWith(floatInterval4));
		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval1));

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval1));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval4.isOverlappingWith(floatInterval4));
	}
	
	/**
	 * Test overlap method (half open intervals)
	 */
	@Test
	public void intervalConvered3() {
		final FloatInterval floatInterval1 = new FloatInterval(1, 100, false, true);
		final FloatInterval floatInterval2 = new FloatInterval(50, 150, false, true);
		final FloatInterval floatInterval3 = new FloatInterval(0, 101, false, true);
		final FloatInterval floatInterval4 = new FloatInterval(100, 101, false, true);

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval1));
		Assert.assertFalse(floatInterval1.isOverlappingWith(floatInterval4));
		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval1));

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval1));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval4.isOverlappingWith(floatInterval4));
	}
	
	/**
	 * Test overlap method (half open intervals)
	 */
	@Test
	public void intervalConvered4() {
		final FloatInterval floatInterval1 = new FloatInterval(1, 100, true, false);
		final FloatInterval floatInterval2 = new FloatInterval(50, 150, true, false);
		final FloatInterval floatInterval3 = new FloatInterval(0, 101, true, false);
		final FloatInterval floatInterval4 = new FloatInterval(100, 101, true, false);

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval1));
		Assert.assertFalse(floatInterval1.isOverlappingWith(floatInterval4));
		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval1));

		Assert.assertTrue(floatInterval1.isOverlappingWith(floatInterval1));
		Assert.assertTrue(floatInterval2.isOverlappingWith(floatInterval2));
		Assert.assertTrue(floatInterval3.isOverlappingWith(floatInterval3));
		Assert.assertTrue(floatInterval4.isOverlappingWith(floatInterval4));
	}
	
}

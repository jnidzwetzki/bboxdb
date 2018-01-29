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

import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DoubleInterval;
import org.junit.Assert;
import org.junit.Test;

public class TestDoubleInterval {
	
	/**
	 * Create an invalid interval
	 */
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void invalidInterval1() {
		final DoubleInterval floatInterval = new DoubleInterval(-5, -10);
	}

	/**
	 * Create an invalid interval
	 */
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void invalidInterval2() {
		final DoubleInterval floatInterval = new DoubleInterval(-5, -5, false, false);
	}
	
	/**
	 * Test cover method
	 */
	@Test
	public void coveredPoint1() {
		final DoubleInterval floatInterval = new DoubleInterval(1, 100);
		Assert.assertTrue(floatInterval.isBeginIncluded());
		Assert.assertTrue(floatInterval.isEndIncluded());
		Assert.assertTrue(floatInterval.overlapsWith(floatInterval.getBegin(), true));
		Assert.assertTrue(floatInterval.overlapsWith(floatInterval.getEnd(), true));
		Assert.assertFalse(floatInterval.overlapsWith(floatInterval.getBegin(), false));
		Assert.assertFalse(floatInterval.overlapsWith(floatInterval.getEnd(), false));
		Assert.assertTrue(floatInterval.overlapsWith(50, true));
		Assert.assertTrue(floatInterval.overlapsWith(50, false));

		Assert.assertFalse(floatInterval.overlapsWith(-10, true));
		Assert.assertFalse(floatInterval.overlapsWith(110, true));
	}
	
	/**
	 * Test cover method
	 */
	@Test
	public void coveredPoint2() {
		final DoubleInterval floatInterval = new DoubleInterval(1, 100, false, false);
		Assert.assertFalse(floatInterval.isBeginIncluded());
		Assert.assertFalse(floatInterval.isEndIncluded());
		Assert.assertFalse(floatInterval.overlapsWith(floatInterval.getBegin(), true));
		Assert.assertFalse(floatInterval.overlapsWith(floatInterval.getEnd(), true));
		Assert.assertFalse(floatInterval.overlapsWith(floatInterval.getBegin(), false));
		Assert.assertFalse(floatInterval.overlapsWith(floatInterval.getEnd(), false));
		Assert.assertTrue(floatInterval.overlapsWith(50, true));
		Assert.assertTrue(floatInterval.overlapsWith(50, false));
		
		Assert.assertFalse(floatInterval.overlapsWith(-10, true));
		Assert.assertFalse(floatInterval.overlapsWith(110, true));
	}
	
	/**
	 * Test overlap method (closed intervals)
	 */
	@Test
	public void intervalCovered1() {
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 100);
		final DoubleInterval floatInterval2 = new DoubleInterval(50, 150);
		final DoubleInterval floatInterval3 = new DoubleInterval(500, 900);
		final DoubleInterval floatInterval4 = new DoubleInterval(100, 101);

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
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 100, false, false);
		final DoubleInterval floatInterval2 = new DoubleInterval(50, 150, false, false);
		final DoubleInterval floatInterval3 = new DoubleInterval(0, 101, false, false);
		final DoubleInterval floatInterval4 = new DoubleInterval(100, 101, false, false);

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
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 100, false, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(50, 150, false, true);
		final DoubleInterval floatInterval3 = new DoubleInterval(0, 101, false, true);
		final DoubleInterval floatInterval4 = new DoubleInterval(100, 101, false, true);

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
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 100, true, false);
		final DoubleInterval floatInterval2 = new DoubleInterval(50, 150, true, false);
		final DoubleInterval floatInterval3 = new DoubleInterval(0, 101, true, false);
		final DoubleInterval floatInterval4 = new DoubleInterval(100, 101, true, false);

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
	 * Test the splitting of intervals
	 */
	@Test
	public void testIntervalSplit1() {
		final DoubleInterval interval1 = new DoubleInterval(0, 100);
		final DoubleInterval leftPart = interval1.splitAndGetLeftPart(50, true);
		final DoubleInterval rightPart = interval1.splitAndGetRightPart(50, true);
		
		Assert.assertTrue(interval1.isBeginIncluded());
		Assert.assertTrue(interval1.isEndIncluded());
		
		Assert.assertTrue(leftPart.isBeginIncluded());
		Assert.assertTrue(leftPart.isEndIncluded());
		
		Assert.assertTrue(rightPart.isBeginIncluded());
		Assert.assertTrue(rightPart.isEndIncluded());
		
		Assert.assertEquals(0, leftPart.getBegin(), 0.0001);
		Assert.assertEquals(50, leftPart.getEnd(), 0.0001);
		
		Assert.assertEquals(50, rightPart.getBegin(), 0.0001);
		Assert.assertEquals(100, rightPart.getEnd(), 0.0001);
	}
	
	/**
	 * Test split at invalid position
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testIntervalSplit2() {
		final DoubleInterval interval1 = new DoubleInterval(0, 100);
		interval1.splitAndGetLeftPart(-10, false);
	}
	
	/**
	 * Test split at invalid position
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testIntervalSplit3() {
		final DoubleInterval interval1 = new DoubleInterval(0, 100);
		interval1.splitAndGetLeftPart(101, false);
	}
	
	/**
	 * Test split at invalid position
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testIntervalSplit4() {
		final DoubleInterval interval1 = new DoubleInterval(0, 100, false, false);
		interval1.splitAndGetLeftPart(0, false);
	}
	
	/**
	 * Test split at invalid position
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testIntervalSplit5() {
		final DoubleInterval interval1 = new DoubleInterval(0, 100, false, false);
		interval1.splitAndGetLeftPart(0, false);
	}
	
	/**
	 * Test midpoint calculation
	 */
	@Test
	public void testMidpoint1() {
		final DoubleInterval interval1 = new DoubleInterval(BoundingBox.MIN_VALUE, BoundingBox.MAX_VALUE);
		Assert.assertEquals(0, interval1.getMidpoint(), 0.0001f);
	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection1() {
		final DoubleInterval floatInterval = new DoubleInterval(1, 2);
		Assert.assertTrue(floatInterval.getIntersection(null) == null);
		Assert.assertEquals(floatInterval.getIntersection(floatInterval), floatInterval);
	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection2() {
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 2, false, false);
		final DoubleInterval floatInterval2 = new DoubleInterval(2, 3, false, false);
		Assert.assertTrue(floatInterval1.getIntersection(floatInterval2) == null);
	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection3() {
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 2, true, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(2, 3, true, true);
		Assert.assertTrue(floatInterval1.getIntersection(floatInterval2) != null);
		Assert.assertTrue(floatInterval2.getIntersection(floatInterval1) != null);

		final DoubleInterval floatIntervalResult = new DoubleInterval(2, 2, true, true);
		Assert.assertEquals(floatIntervalResult, floatInterval1.getIntersection(floatInterval2));
		Assert.assertEquals(floatIntervalResult, floatInterval2.getIntersection(floatInterval1));

	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection4() {
		final DoubleInterval floatInterval1 = new DoubleInterval(5, 10, true, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(1, 11, true, true);
		Assert.assertTrue(floatInterval1.getIntersection(floatInterval2) != null);
		Assert.assertEquals(floatInterval1, floatInterval1.getIntersection(floatInterval2));
		Assert.assertTrue(floatInterval2.getIntersection(floatInterval1) != null);
		Assert.assertEquals(floatInterval1, floatInterval2.getIntersection(floatInterval1));
	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection5() {
		final DoubleInterval floatInterval2 = new DoubleInterval(5, 10, true, true);
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 11, true, true);
		Assert.assertTrue(floatInterval2.getIntersection(floatInterval1) != null);
		Assert.assertEquals(floatInterval2, floatInterval2.getIntersection(floatInterval1));
		Assert.assertTrue(floatInterval1.getIntersection(floatInterval2) != null);
		Assert.assertEquals(floatInterval2, floatInterval1.getIntersection(floatInterval2));
	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection6() {
		final DoubleInterval floatInterval2 = new DoubleInterval(16, 20, true, true);
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 11, true, true);
		Assert.assertTrue(floatInterval2.getIntersection(floatInterval1) == null);
		Assert.assertTrue(floatInterval1.getIntersection(floatInterval2) == null);
	}
	
	
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection7() {
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 2, true, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(1, 3, true, true);
		Assert.assertEquals(floatInterval1, floatInterval2.getIntersection(floatInterval1));
		Assert.assertEquals(floatInterval1, floatInterval1.getIntersection(floatInterval2));
	}
	
	/**
	 * Test intersection calculation
	 */
	@Test
	public void testIntersection8() {
		final DoubleInterval floatInterval1 = new DoubleInterval(2, 3, true, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(1, 3, true, true);
		Assert.assertEquals(floatInterval1, floatInterval2.getIntersection(floatInterval1));
		Assert.assertEquals(floatInterval1, floatInterval1.getIntersection(floatInterval2));
	}
	
	/**
	 * The the covering method
	 */
	@Test
	public void testIsCovered1() {
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 2, true, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(1.5, 3, true, true);
		final DoubleInterval floatInterval3 = new DoubleInterval(0, 3, true, true);

		Assert.assertFalse(floatInterval1.isCovering(floatInterval2));
		Assert.assertFalse(floatInterval1.isCovering(floatInterval3));
		
		Assert.assertTrue(floatInterval3.isCovering(floatInterval1));
		Assert.assertTrue(floatInterval3.isCovering(floatInterval2));
	}
	
	/**
	 * The the covering method
	 */
	@Test
	public void testIsCovered2() {
		final DoubleInterval floatInterval1 = new DoubleInterval(1, 2, true, true);
		final DoubleInterval floatInterval2 = new DoubleInterval(1, 2, false, false);
		final DoubleInterval floatInterval3 = new DoubleInterval(1, 2, false, true);
		final DoubleInterval floatInterval4 = new DoubleInterval(1, 2, true, false);

		Assert.assertTrue(floatInterval1.isCovering(floatInterval1));
		Assert.assertTrue(floatInterval1.isCovering(floatInterval2));
		Assert.assertTrue(floatInterval1.isCovering(floatInterval3));
		Assert.assertTrue(floatInterval1.isCovering(floatInterval4));
		
		Assert.assertFalse(floatInterval2.isCovering(floatInterval1));
		Assert.assertTrue(floatInterval2.isCovering(floatInterval2));
		Assert.assertFalse(floatInterval2.isCovering(floatInterval3));
		Assert.assertFalse(floatInterval2.isCovering(floatInterval4));
		
		Assert.assertFalse(floatInterval3.isCovering(floatInterval1));
		Assert.assertTrue(floatInterval3.isCovering(floatInterval2));
		Assert.assertTrue(floatInterval1.isCovering(floatInterval3));
		Assert.assertFalse(floatInterval3.isCovering(floatInterval4));

		Assert.assertFalse(floatInterval4.isCovering(floatInterval1));
		Assert.assertTrue(floatInterval4.isCovering(floatInterval2));
		Assert.assertFalse(floatInterval4.isCovering(floatInterval3));
		Assert.assertTrue(floatInterval1.isCovering(floatInterval4));
	}
	
}

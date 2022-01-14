/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHyperrectangleHelper {

	protected final static float EQUALS_DELTA = 0.001f;

	@BeforeClass
	public static void beforeClass() {
		Hyperrectangle.enableChecks = true;
	}

	@AfterClass
	public static void afterClass() {
		Hyperrectangle.enableChecks = false;
	}

	@Test(timeout=60_000, expected = IllegalArgumentException.class)
	public void testFullSpaceGenerationNegative() {
		HyperrectangleHelper.getFullSpaceForDimension(-1, 0, 12);
	}
	
	@Test(timeout=60_000, expected = IllegalArgumentException.class)
	public void testFullSpaceGenerationDimZero() {
		HyperrectangleHelper.getFullSpaceForDimension(0, 0, 12);
	}
	
	@Test(timeout=60_000, expected = IllegalArgumentException.class)
	public void testFullSpaceGenerationDimWrongValues1() {
		HyperrectangleHelper.getFullSpaceForDimension(1, 100, 0);
	}
	
	@Test(timeout=60_000, expected = IllegalArgumentException.class)
	public void testFullSpaceGenerationDimWrongValues2() {
		HyperrectangleHelper.getFullSpaceForDimension(1, 100, 100);
	}
	
	@Test(timeout=60_000)
	public void testFullSpaceGenerationCorrect1() {
		final Hyperrectangle hyperrectangle = HyperrectangleHelper.getFullSpaceForDimension(1, 0, 100);
		
		Assert.assertEquals(1, hyperrectangle.getDimension());
		Assert.assertEquals(0, hyperrectangle.getCoordinateLow(0), EQUALS_DELTA);
		Assert.assertEquals(100, hyperrectangle.getCoordinateHigh(0), EQUALS_DELTA);
	}
	
	@Test(timeout=60_000)
	public void testFullSpaceGenerationCorrect2() {
		final Hyperrectangle hyperrectangle = HyperrectangleHelper.getFullSpaceForDimension(2, 0, 100);
		
		Assert.assertEquals(2, hyperrectangle.getDimension());
		
		for(int i = 0; i < hyperrectangle.getDimension(); i++) {
			Assert.assertEquals(0, hyperrectangle.getCoordinateLow(i), EQUALS_DELTA);
			Assert.assertEquals(100, hyperrectangle.getCoordinateHigh(i), EQUALS_DELTA);
		}
	}
	
	@Test(timeout=60_000)
	public void testFullSpaceGenerationCorrect3() {
		final Hyperrectangle hyperrectangle = HyperrectangleHelper.getFullSpaceForDimension(3, -1000, 1000);
		
		Assert.assertEquals(3, hyperrectangle.getDimension());
		
		for(int i = 0; i < hyperrectangle.getDimension(); i++) {
			Assert.assertEquals(-1000, hyperrectangle.getCoordinateLow(i), EQUALS_DELTA);
			Assert.assertEquals(1000, hyperrectangle.getCoordinateHigh(i), EQUALS_DELTA);
		}
	}
}

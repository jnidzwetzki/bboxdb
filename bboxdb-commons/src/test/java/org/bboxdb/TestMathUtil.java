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
package org.bboxdb;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestMathUtil {

	/**
	 * The default assert delta
	 */
	private static final double DEFAULT_ASSERT_DELTA = 0.0000001;

	@Test
	public void testRound() {
		final double d1 = 1.5;
		final double d2 = 1.51;
		final double d3 = 1.521;
		final double d4 = 1.5334;
		final double d5 = 1.54432;
		final double d6 = 1.534321;
		final double d7 = 1.534344444;
		final double d8 = 1.534399999;

		Assert.assertEquals(d1, MathUtil.round(d1, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(d2, MathUtil.round(d2, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(d3, MathUtil.round(d3, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(d4, MathUtil.round(d4, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(d5, MathUtil.round(d5, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(1.53432, MathUtil.round(d6, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(1.53434, MathUtil.round(d7, 5), DEFAULT_ASSERT_DELTA);
		Assert.assertEquals(1.53440, MathUtil.round(d8, 5), DEFAULT_ASSERT_DELTA);
	}
	
	@Test
	public void testParseInteger(){
		try {
			MathUtil.tryParseInt("abc", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			MathUtil.tryParseInt(null, MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			final int result2 = MathUtil.tryParseInt("234", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertEquals(234, result2);
		} catch (InputParseException e) {
			Assert.assertTrue(false);
		}
	}

	
	@Test
	public void testParseDouble() {
		try {
			MathUtil.tryParseDouble("abc", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			MathUtil.tryParseDouble(null, MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			final double result2 = MathUtil.tryParseDouble("234.567", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertEquals(234.567, result2, DEFAULT_ASSERT_DELTA);
		} catch (InputParseException e) {
			Assert.assertTrue(false);
		}
	}
	
	@Test
	public void testParseLong() {
		try {
			MathUtil.tryParseLong("abc", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			MathUtil.tryParseLong(null, MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			MathUtil.tryParseLong("3.14159", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			final long result2 = MathUtil.tryParseLong("454545435", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertEquals(454545435l, result2);
		} catch (InputParseException e) {
			Assert.assertTrue(false);
		}
	}
	
	@Test
	public void testParseBoolean() {
		try {
			MathUtil.tryParseBoolean("true1234", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			MathUtil.tryParseBoolean(null, MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(false);
		} catch(InputParseException e) {
			
		}
		
		try {
			final boolean result1 = MathUtil.tryParseBoolean("true", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(result1);
			
			final boolean result2 = MathUtil.tryParseBoolean("TRUE", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(result2);
			
			final boolean result3 = MathUtil.tryParseBoolean("TrUE", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(result3);
			
			final boolean result4 = MathUtil.tryParseBoolean("1", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertTrue(result4);
			
			final boolean result5 = MathUtil.tryParseBoolean("false", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertFalse(result5);
			
			final boolean result6 = MathUtil.tryParseBoolean("FALSE", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertFalse(result6);
			
			final boolean result7 = MathUtil.tryParseBoolean("false", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertFalse(result7);
			
			final boolean result8 = MathUtil.tryParseBoolean("0", MathUtil.DEFAULT_ERROR_SUPPLIER);
			Assert.assertFalse(result8);
		} catch (InputParseException e) {
			Assert.assertTrue(false);
		}
	}
}

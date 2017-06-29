/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import org.bboxdb.util.MathUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestMathUtil {

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

		Assert.assertEquals(d1, MathUtil.round(d1, 5), 0.0000001);
		Assert.assertEquals(d2, MathUtil.round(d2, 5), 0.0000001);
		Assert.assertEquals(d3, MathUtil.round(d3, 5), 0.0000001);
		Assert.assertEquals(d4, MathUtil.round(d4, 5), 0.0000001);
		Assert.assertEquals(d5, MathUtil.round(d5, 5), 0.0000001);
		Assert.assertEquals(1.53432, MathUtil.round(d6, 5), 0.0000001);
		Assert.assertEquals(1.53434, MathUtil.round(d7, 5), 0.0000001);
		Assert.assertEquals(1.53440, MathUtil.round(d8, 5), 0.0000001);
	}

}

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
package org.bboxdb;

import org.bboxdb.commons.StringUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestStringUtil {

	/**
	 * Test the countCharOccurrence method
	 */
	@Test(timeout=60000)
	public void testcountCharOccurrence() {
		Assert.assertEquals(0, StringUtil.countCharOccurrence("abc", '_'));

		Assert.assertEquals(1, StringUtil.countCharOccurrence("abc_", '_'));
		Assert.assertEquals(1, StringUtil.countCharOccurrence("_abc", '_'));
		Assert.assertEquals(1, StringUtil.countCharOccurrence("ab_c", '_'));
		Assert.assertEquals(1, StringUtil.countCharOccurrence("_", '_'));

		Assert.assertEquals(2, StringUtil.countCharOccurrence("__", '_'));
		Assert.assertEquals(2, StringUtil.countCharOccurrence("_abc_", '_'));
		Assert.assertEquals(2, StringUtil.countCharOccurrence("a_b_c", '_'));
		Assert.assertEquals(2, StringUtil.countCharOccurrence("abc__", '_'));
	}
}

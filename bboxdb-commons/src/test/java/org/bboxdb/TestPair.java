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

import org.bboxdb.commons.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestPair {

	/**
	 * Test the pair class
	 */
	@Test(timeout=60000)
	public void testPair() {
		final String string1 = "abc";
		final String string2 = "123";
		
		final Pair<String, String> pair1 = new Pair<String, String>(string1, string2);
		final Pair<String, String> pair2 = new Pair<String, String>(string1, string2);

		Assert.assertEquals(string1, pair1.getElement1());
		Assert.assertEquals(string2, pair1.getElement2());
		Assert.assertTrue(pair1.toString().length() > 10);
		Assert.assertEquals(pair1, pair2);
		Assert.assertEquals(pair1.hashCode(), pair2.hashCode());

		pair1.setElement1("123");
		pair2.setElement2("abc");
		Assert.assertFalse(pair1.equals(pair2));
	}
}

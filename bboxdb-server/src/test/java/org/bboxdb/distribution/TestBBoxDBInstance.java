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
package org.bboxdb.distribution;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.junit.Assert;
import org.junit.Test;

public class TestBBoxDBInstance {

	@Test(timeout=60000)
	public void testCompare() {
		final BBoxDBInstance bBoxDBInstance1 = new BBoxDBInstance("127.0.0.1:10000");
		final BBoxDBInstance bBoxDBInstance2 = new BBoxDBInstance("127.0.0.2:10000");
		Assert.assertTrue(bBoxDBInstance1.compareTo(bBoxDBInstance2) < 0);
		Assert.assertTrue(bBoxDBInstance2.compareTo(bBoxDBInstance1) > 0);
		Assert.assertTrue(bBoxDBInstance1.compareTo(bBoxDBInstance1) == 0);
	}
	
	@Test(timeout=60000)
	public void testToString() {
		final BBoxDBInstance bBoxDBInstance1 = new BBoxDBInstance("127.0.0.1:10000");
		Assert.assertTrue(bBoxDBInstance1.getStringValue().length() > 0);
		Assert.assertTrue(bBoxDBInstance1.toGUIString(false).length() > 0);
		Assert.assertTrue(bBoxDBInstance1.toGUIString(true).length() > 0);
	}
	
	@Test(timeout=60000)
	public void testGetter() {
		final BBoxDBInstance bBoxDBInstance1 = new BBoxDBInstance("127.0.0.1:10000");
		Assert.assertEquals("127.0.0.1", bBoxDBInstance1.getIp());
		Assert.assertEquals(10000, bBoxDBInstance1.getPort());
		Assert.assertEquals(BBoxDBInstance.UNKOWN_PROPERTY, bBoxDBInstance1.getVersion());
	}
	
	@Test(timeout=60000)
	public void testSocketEquals() {
		final BBoxDBInstance bBoxDBInstance1 = new BBoxDBInstance("127.0.0.1:10000");
		Assert.assertFalse(bBoxDBInstance1.socketAddressEquals(null));
	}
}

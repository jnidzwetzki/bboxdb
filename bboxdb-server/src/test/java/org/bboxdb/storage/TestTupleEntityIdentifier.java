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
package org.bboxdb.storage;

import org.bboxdb.storage.entity.TupleEntityIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class TestTupleEntityIdentifier {

	@Test(timeout=60000)
	public void testGetSetter() {
		final TupleEntityIdentifier tupleEntityIdentifier = new TupleEntityIdentifier("abc", 1234);
		Assert.assertEquals("abc", tupleEntityIdentifier.getKey());
		Assert.assertEquals(1234, tupleEntityIdentifier.getVersion());
		Assert.assertTrue(tupleEntityIdentifier.toString().length() > 10);
	}
}

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

import java.io.IOException;

import org.bboxdb.commons.ObjectSerializer;
import org.bboxdb.commons.entity.PersonEntity;
import org.junit.Assert;
import org.junit.Test;

public class TestObjectSerializer {
	
	@Test(timeout=60000)
	public void testSerializer1() throws ClassNotFoundException, IOException {
		final String test = "This is a test";
		ObjectSerializer<String> serializer = new ObjectSerializer<String>();
		Assert.assertEquals(test, serializer.deserialize(serializer.serialize(test)));
	}
	
	@Test(timeout=60000)
	public void testSerializer2() throws ClassNotFoundException, IOException {
		final PersonEntity test = new PersonEntity("Jan", "Jensen", 30);
		ObjectSerializer<PersonEntity> serializer = new ObjectSerializer<PersonEntity>();
		final PersonEntity deserialize = serializer.deserialize(serializer.serialize(test));
		Assert.assertEquals(test, deserialize);
		Assert.assertTrue(test.toString().length() > 10);
		Assert.assertEquals(test.hashCode(), deserialize.hashCode());
	}
	
}

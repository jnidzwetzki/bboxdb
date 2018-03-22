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

import java.io.File;

import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestTuplestoreConfiguration {
	
	@Test(timeout=60000)
	public void testWriteAndReadConfiguration() {
		final TupleStoreConfiguration configuration1 = new TupleStoreConfiguration();
		final String yamlString = configuration1.exportToYaml();
		final TupleStoreConfiguration configuration2 = TupleStoreConfiguration.importFromYaml(yamlString);
		
		Assert.assertEquals(configuration1, configuration2);
		Assert.assertEquals(configuration1.hashCode(), configuration2.hashCode());
		Assert.assertTrue(configuration1.toString().length() > 10);
	}

	@Test(timeout=60000)
	public void testReadNonExistingFile() {
		final File tmpFile = new File("/tmp/tuplestore.nonexisting");
		Assert.assertTrue(TupleStoreConfiguration.importFromYamlFile(tmpFile) == null);
	}
}

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

import java.io.File;

import org.bboxdb.commons.io.FileUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestFileUtil {
	
	@Test(timeout=60000)
	public void testNonExisting() {
		final File file = new File("/tmp/deletion");
		Assert.assertFalse(file.exists());
		FileUtil.deleteRecursive(file.toPath());
		Assert.assertFalse(file.exists());
	}
	
	@Test(timeout=60000)
	public void testNonExisting1() {
		final File file = new File("/tmp/deletion");
		file.mkdirs();
		Assert.assertTrue(file.exists());
		FileUtil.deleteRecursive(file.toPath());
		Assert.assertFalse(file.exists());
	}
	
	@Test(timeout=60000)
	public void testNonExisting2() {
		final File file = new File("/tmp/deletion");
		file.mkdirs();
		
		final File file1 = new File("/tmp/deletion/subdir1/subdir2/subdir3");
		file1.mkdir();
		
		final File file2 = new File("/tmp/deletion/subdira/subdirb/subdirc");
		file2.mkdir();
	
		Assert.assertTrue(file.exists());
		FileUtil.deleteRecursive(file.toPath());
		Assert.assertFalse(file.exists());
	}
}

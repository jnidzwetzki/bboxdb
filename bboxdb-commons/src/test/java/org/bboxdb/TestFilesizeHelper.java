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

import org.bboxdb.commons.FileSizeHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestFilesizeHelper {

	@Test(expected=IllegalArgumentException.class)
	public void testFileSizeInvalid() {
		FileSizeHelper.readableFileSize(-2);
	}
	
	@Test
	public void testFileSize() {
		final String bytes = FileSizeHelper.readableFileSize(3);
		Assert.assertEquals("3 B", bytes);
		final String kbytes = FileSizeHelper.readableFileSize(2048);
		Assert.assertEquals("2 KB", kbytes);
		final String mbytes = FileSizeHelper.readableFileSize(4194304);
		Assert.assertEquals("4 MB", mbytes);
	}
}

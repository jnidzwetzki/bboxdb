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

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.bboxdb.commons.io.UnsafeMemoryHelper;
import org.junit.Assert;
import org.junit.Test;

public class TestUnsafeMemoryHandler {
	
	/**
	 * Test the unmapping of memory mapped files (if available in JVM)
	 * @throws Exception
	 */
	@Test(timeout=60000)
	public void testUnsafeMemoryHandler() throws Exception {
		UnsafeMemoryHelper.testMemoryUnmapperAvailable();
		
		if(! UnsafeMemoryHelper.isDirectMemoryUnmapperAvailable()) {
			return;
		}
	
		final long oldBytes = UnsafeMemoryHelper.getMappedBytes();
		final long oldSegments = UnsafeMemoryHelper.getMappedSegments();
		
		UnsafeMemoryHelper.unmapMemory(null);
		
		final RandomAccessFile randomAccessFile = new RandomAccessFile("/tmp/mapfile", "rw");
		final MappedByteBuffer mappedBytes = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 100);
		   
		Assert.assertEquals(oldBytes + 100, UnsafeMemoryHelper.getMappedBytes());
		Assert.assertEquals(oldSegments + 1, UnsafeMemoryHelper.getMappedSegments());
		
		randomAccessFile.close();
		UnsafeMemoryHelper.unmapMemory(mappedBytes);

		Assert.assertEquals(oldBytes, UnsafeMemoryHelper.getMappedBytes());
		Assert.assertEquals(oldSegments, UnsafeMemoryHelper.getMappedSegments());
	}
	
}

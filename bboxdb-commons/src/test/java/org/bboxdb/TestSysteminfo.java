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

import org.bboxdb.commons.SystemInfo;
import org.junit.Assert;
import org.junit.Test;

public class TestSysteminfo {

	/**
	 * Test the system info
	 */
	@Test(timeout=60000)
	public void testSysteminfo() {
		Assert.assertTrue(SystemInfo.getAvailableMemory() >= 1);
		Assert.assertTrue(SystemInfo.getCPUCores() >= 1);
		Assert.assertTrue(SystemInfo.getFreeDiskspace(new File("/")) >= 1);
		Assert.assertTrue(SystemInfo.getTotalDiskspace(new File("/")) >= 1);
	}
	
	/**
	 * Test the system info
	 */
	@Test(timeout=60000)
	public void testMemoryInfo() {
		final String memoryStatisticsString = SystemInfo.getMemoryStatisticsString();
		System.out.println(memoryStatisticsString);
		Assert.assertTrue(memoryStatisticsString.length() > 10);
	}
	
}

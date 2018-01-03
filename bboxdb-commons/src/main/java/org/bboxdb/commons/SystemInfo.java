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
package org.bboxdb.commons;

import java.io.File;

public class SystemInfo {

	/**
	 * Get the amount of CPU cores in this system
	 * @return
	 */
	public static int getCPUCores() {
		return Runtime.getRuntime().availableProcessors();
	}
	
	/**
	 * Get the amount of available memory for this JVM
	 * @return
	 */
	public static long getAvailableMemory() {
		return Runtime.getRuntime().maxMemory();
	}
	
	/**
	 * Get the total diskspace on the location
	 * @param path
	 * @return
	 */
	public static long getTotalDiskspace(final File path) {
		return path.getTotalSpace();
	}
	
	/**
	 * Get the free diskspace on the location
	 * @param location
	 * @return
	 */
	public static long getFreeDiskspace(final File path) {
		return path.getUsableSpace();
	}

}

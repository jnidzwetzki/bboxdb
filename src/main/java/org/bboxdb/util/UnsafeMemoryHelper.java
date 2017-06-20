/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.util;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public class UnsafeMemoryHelper {
	
	/**
	 * Is the direct memory unmapper available? (Oracle JVM specific)
	 */
	protected final static boolean directMemoryUnmapperAvailable;
	
	static {
		directMemoryUnmapperAvailable = testMemoryUnmapperAvailable();
	}

	/**
	 * Run the memory unmapper check
	 * @return
	 */
	public static boolean testMemoryUnmapperAvailable() {
		try {
			final ByteBuffer buf = ByteBuffer.allocateDirect(1);
			((DirectBuffer) buf).cleaner().clean();
			return true;
		} catch (Throwable t) {
			return false;
		}		
	}
	
	/**
	 * Is the direct memory unmapper available?
	 * @return
	 */
	public static boolean isDirectMemoryUnmapperAvailable() {
		return directMemoryUnmapperAvailable;
	}
	
	/**
	 * Unmap the given memory byte buffer
	 * @param memory
	 */
	public static void unmapMemory(final MappedByteBuffer memory) {
		if(memory == null) {
			return;
		}
		
		if(memory.isDirect()) {
			((DirectBuffer) memory).cleaner().clean();
		}
	}

}

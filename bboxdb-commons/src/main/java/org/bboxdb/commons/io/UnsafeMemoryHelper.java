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
package org.bboxdb.commons.io;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnsafeMemoryHelper {
	
	/**
	 * THe name of the memory mapped mbean
	 */
	private static final String MBEAN_NAME = "java.nio:type=BufferPool,name=mapped";
	
	/**
	 * When available (e.g., on Oracle JVM) the method contains a reference to the memory cleaner
	 */
    private static MethodHandle memoryClean;
    
    /**
     * Reference to the sun.nio.ch.DirectBuffer#cleaner
     */
    private static MethodHandle memoryCleaner;
    
    /**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(UnsafeMemoryHelper.class);
	
	
	static {
		// The internal class it not always available.
		// Use reflection to determine the feature at runtime and to prevent a 
		// static binding to the internal class "sun.nio.ch.DirectBuffer"
		
		// Direct Oracle JDK 8 code:
		// ((DirectBuffer) buf).cleaner().clean();
		
		try {
			final Class<?> cleanerClass = Class.forName("sun.nio.ch.DirectBuffer");
			final Method cleanderMethod = cleanerClass.getMethod("cleaner");
			memoryCleaner = MethodHandles.lookup().unreflect(cleanderMethod);
			final Method cleanMethod = cleanderMethod.getReturnType().getMethod("clean");
			memoryClean = MethodHandles.lookup().unreflect(cleanMethod);
			
			// Test unmapping
			final ByteBuffer buf = ByteBuffer.allocateDirect(1);
			final boolean result = unmapMemory(buf);
			
			if(! result) {
				memoryUnmapperNotAvailable();
			} else {
				logger.info("Optimized memory handling is available");
			}
			
		} catch (Exception e) {
			logger.warn("Unable to detect memory cleaner, direct cleaning of memory mapped io does not work");
			memoryUnmapperNotAvailable();
		} 
	}
	
	/**
	 * Set the memory cleaner to not available
	 */
	private static void memoryUnmapperNotAvailable() {
		memoryClean = null;
		memoryCleaner = null;
	}
	
	/**
	 * Is the direct memory unmapper available?
	 * @return
	 */
	public static boolean isDirectMemoryUnmapperAvailable() {
		return (memoryClean != null) && (memoryCleaner != null);
	}
	
	/**
	 * Unmap the given memory byte buffer
	 * @param memory
	 */
	public static boolean unmapMemory(final ByteBuffer memory) {
		if(memory == null) {
			return false;
		}
		
		if(memory.isDirect() && isDirectMemoryUnmapperAvailable()) {
			try {
	            final Object cleanerBuffer = memoryCleaner.bindTo(memory).invoke();
				memoryClean.bindTo(cleanerBuffer).invoke();
				return true;
			} catch (Throwable e) {
				logger.warn("Unable to unmap memory", e);
			}
		}
		
		return false;
	}

	/**
	 * Get the number of mapped segments
	 * @return
	 * @throws Exception
	 */
	public static long getMappedSegments() throws Exception {
		final ObjectName objectName = new ObjectName(MBEAN_NAME);
		final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
		final Long mmapCount = (Long) mbeanServer.getAttribute(objectName, "Count");
		
		return mmapCount.longValue();
	}
	
	/**
	 * Get the number of mapped bytes
	 * @return
	 * @throws Exception
	 */
	public static long getMappedBytes() throws Exception {
		final ObjectName objectName = new ObjectName(MBEAN_NAME);
		final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
		final Long mmapMemoryUsed = (Long) mbeanServer.getAttribute(objectName, "MemoryUsed");
		
		return mmapMemoryUsed.longValue();
	}
	
}

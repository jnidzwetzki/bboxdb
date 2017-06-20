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
package org.bboxdb.tools.experiments;

import java.lang.management.ManagementFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Test the behavior of the VM memory when
 * memory mapped files are used
 *
 */
public class MemoryMappedFiles {
	
	/**
	 * Print the JVM memory statistics
	 */
	protected static void printMemoryStatistics() {
		final long maxMemory = Runtime.getRuntime().maxMemory();
		System.out.println("Maximum memory (bytes): " + 
				  (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

		System.out.println("Total memory (bytes): " + 
		Runtime.getRuntime().totalMemory());
		  
		System.out.println("Free memory within total (bytes): " + 
		Runtime.getRuntime().freeMemory());
	}
	
	/**
	 * Print the memory mapped MBean data
	 * @param args
	 * @throws MalformedObjectNameException 
	 * @throws MBeanException 
	 * @throws ReflectionException 
	 * @throws AttributeNotFoundException 
	 * @throws InstanceNotFoundException 
	 */
	protected static void printMappedStatistics() throws MalformedObjectNameException, 
			InstanceNotFoundException, AttributeNotFoundException, ReflectionException, MBeanException {
		
		final ObjectName objectName = new ObjectName("java.nio:type=BufferPool,name=mapped");
		final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
		final Long mmapCount = (Long) mbeanServer.getAttribute(objectName, "Count");
	    final Long mmapMemoryUsed = (Long) mbeanServer.getAttribute(objectName, "MemoryUsed");

	    System.out.format("Number of mmaps %d number of mmap memory %d", 
	    		mmapCount.longValue(), mmapMemoryUsed.longValue());
	}

	/**
	 * Main * main * main * main * main * main
	 * @param args
	 * @throws MBeanException 
	 * @throws ReflectionException 
	 * @throws AttributeNotFoundException 
	 * @throws InstanceNotFoundException 
	 * @throws MalformedObjectNameException 
	 */
	public static void main(final String[] args) throws Exception {
		printMemoryStatistics();
		printMappedStatistics();
	}

}

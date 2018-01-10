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
package org.bboxdb.experiments;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.io.UnsafeMemoryHelper;

/**
 * Test the behavior of the VM memory when memory mapped files are used
 *
 */
public class MemoryMappedFiles {
	
	/**
	 * The size of one MB
	 */
	public static final int MB = 1024 * 1024;

	/**
	 * Print the JVM memory statistics
	 */
	protected static void printMemoryStatistics() {
		final long maxMemory = Runtime.getRuntime().maxMemory();
		
		System.out.println("Maximum memory (bytes): " 
				+ (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

		System.out.println("Total memory (bytes): " 
				+ Runtime.getRuntime().totalMemory());

		System.out.println("Free memory within total (bytes): " 
				+ Runtime.getRuntime().freeMemory());
	}

	/**
	 * Print the memory mapped MBean data
	 * 
	 * @param args
	 * @throws Exception
	 */
	protected static void printMappedStatistics() throws Exception {

		System.out.format("Number of mmaps %d number of mmap memory %d\n", 
				UnsafeMemoryHelper.getMappedSegments(),
				UnsafeMemoryHelper.getMappedBytes());
		
		final boolean unmapperState = UnsafeMemoryHelper.isDirectMemoryUnmapperAvailable();
		
		System.out.println("The memory mapped unmapper is available: " + unmapperState);
	}

	/**
	 * Main * main * main * main * main * main
	 * 
	 * @param args
	 * @throws MBeanException
	 * @throws ReflectionException
	 * @throws AttributeNotFoundException
	 * @throws InstanceNotFoundException
	 * @throws MalformedObjectNameException
	 */
	public static void main(final String[] args) throws Exception {
		
		if(args.length != 1) {
			System.err.println("Usage: <Number of files>");
			System.exit(-1);
		}
		
		final int numberOfFiles = MathUtil.tryParseIntOrExit(args[0]);
		
		System.out.println();
		System.out.println("==============");
		System.out.println("After start memory");
		printMemoryStatistics();
		printMappedStatistics();

		final Set<MemoryMappedFile> files = new HashSet<>();
		for(int i = 0; i < numberOfFiles; i++) {
			final int mappingFileSize = 100 * MB;
			final MemoryMappedFile mmf1 = new MemoryMappedFile(mappingFileSize);
			files.add(mmf1);
			System.out.println();
			System.out.println("==============");
			System.out.println("Create new mapping file: " + mmf1.getFile());
			System.out.println("With size: " + mappingFileSize);
	
			// Reopen file
			System.out.println();
			System.out.println("==============");
			System.out.println("Mapping into memory");
			mmf1.map();
			printMemoryStatistics();
			printMappedStatistics();
		}
		
		System.out.println("==============");
		System.out.println("Read");
		files.stream().forEach(f -> f.read());
		printMemoryStatistics();
		printMappedStatistics();
		
		System.out.println("==============");
		System.out.println("Sleep");
		Thread.sleep(TimeUnit.SECONDS.toMillis(20));
		printMemoryStatistics();
		printMappedStatistics();
		
		System.out.println();
		System.out.println("==============");
		System.out.println("Unmapping files");
		for(final MemoryMappedFile mmf : files) {
			mmf.unmap();
		}
		printMemoryStatistics();
		printMappedStatistics();
		
		System.out.println();
		System.out.println("==============");
		System.out.println("Running GC");
		System.gc();
		printMemoryStatistics();
		printMappedStatistics();
	}

}

class MemoryMappedFile {
	
	private File file;
	
	private RandomAccessFile randomAccessFile;
	
	private FileChannel fileChannel;
	
	private MappedByteBuffer memory;

	public MemoryMappedFile(final int filesize) throws IOException {			
		file = File.createTempFile("mmap", ".bin");
		file.deleteOnExit();
		
		randomAccessFile = new RandomAccessFile(file, "rw");
		randomAccessFile.setLength(filesize);
		randomAccessFile.close();
	}
	
	/**
	 * Map file in memory
	 * @throws IOException
	 */
	public void map() throws IOException {
		randomAccessFile = new RandomAccessFile(file, "r");
		fileChannel = randomAccessFile.getChannel();
		
		final long size = fileChannel.size();
		assert file.length() == size;
		
		memory = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
		
		assert memory != null;
		assert memory.isDirect();
	}
	
	/**
	 * Read the memory
	 */
	void read() {
		assert memory != null;
		
		System.out.println("Reading " + file.getAbsolutePath());
		
		memory.position(0);
		
		while(memory.hasRemaining()) {
			memory.get();
		}
	}
	
	/**
	 * Unmap file
	 * @throws IOException
	 */
	public void unmap() throws IOException {
		if(fileChannel != null) {
			fileChannel.close();
			fileChannel = null;
		}
		
		if(randomAccessFile != null) {
			randomAccessFile.close();
			randomAccessFile = null;
		}
		
		if(memory != null) {
			UnsafeMemoryHelper.unmapMemory(memory);
			memory = null;
		}
	}
	
	public File getFile() {
		return file;
	}
}

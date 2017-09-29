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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.google.common.base.Stopwatch;

public class TestFileIO implements Runnable {

	/**
	 * The file
	 */
	private final String filename;
	
	/**
	 * Filesize of 2 GB
	 */
	protected final static int FILESIZE = 2 * 1024 * 1024 * 1024 - 1;
	
	/**
	 * Retry
	 * @param filename
	 */
	protected final static int RETRY = 3;

	/**
	 * The random generator
	 */
	protected final Random random;
	
	protected RandomAccessFile raf;
	
	protected MappedByteBuffer mappedByteBuffer;

	public TestFileIO(final String filename) {
		this.filename = filename;
		random = new Random();
	}
	
	@Override
	public void run() {
		final List<Integer> reads = Arrays.asList(10000, 50000, 100000, 1000000, 5000000, 10000000, 50000000, 100000000);
		
		try {
			generateTestData();
			
			System.out.println("Read requests\tTime random\tTime memory");
			
			for(final int readRequsts : reads) {
				
				final Stopwatch stopwatch = Stopwatch.createStarted();

				for(int i = 0; i < RETRY; i++) {
					readDataRandom(readRequsts);
				}
				
				final int timeRandom = (int) (stopwatch.elapsed().toMillis() / RETRY);
				
				stopwatch.reset();
				for(int i = 0; i < RETRY; i++) {
					readDataMemoryMapped(readRequsts);
				}
				
				final int timeMemory = (int) (stopwatch.elapsed().toMillis() / RETRY);

				System.out.printf("%d\t%d\t%d\n", readRequsts, timeRandom, timeMemory);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} 
	}

	protected void readDataMemoryMapped(final int readRequsts) throws FileNotFoundException, IOException {
	
		int nextPosition = 0;
		try {
			for(int i = 0; i < readRequsts; i++) {
				nextPosition = getNextPosition();
				mappedByteBuffer.position(nextPosition);
				mappedByteBuffer.get();
			}
		} catch(IllegalArgumentException e) {
			e.printStackTrace();
			System.out.println("Exception by setting to: " + nextPosition);
		}
	}

	protected void readDataRandom(final int readRequsts) throws Exception {
		
			for(int i = 0; i < readRequsts; i++) {
				raf.seek(getNextPosition());
				raf.readByte();
			}
	}

	protected int getNextPosition() {
		return Math.abs(random.nextInt()) % FILESIZE;
	}

	protected String getTestDataBuffer(final int length) {
		final StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < length; i++) {
			sb.append("a");
		}
		
		return sb.toString();
	}

	protected void generateTestData() throws Exception {
		System.out.println("# Generating test data");
		final File file = new File(filename);
		file.deleteOnExit();
		
		long writtenBytes = 0;
		final byte[] stringBufferBytes = getTestDataBuffer(1024).getBytes();
		
		try(
				final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
		) {
			while(writtenBytes < FILESIZE) {
				bos.write(stringBufferBytes);
				writtenBytes += stringBufferBytes.length;
			}
			
		} catch (Exception e) {
			throw e;
		}
		
		System.out.println("# File size is now: " + file.length());
		
		raf = new RandomAccessFile(new File(filename), "r");
		mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, FILESIZE);
	}
	
	/**
	 * ====================================================
	 * Main * Main * Main
	 * ====================================================
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 1) {
			System.err.println("Usage: program <filename>");
			System.exit(-1);
		}
		
		final File file = new File(args[0]);
		if(file.exists()) {
			System.err.println("Output file exists, please remove");
			System.exit(-1);
		}
		
		final TestFileIO testFileIO = new TestFileIO(args[0]);
		testFileIO.run();
	}

}

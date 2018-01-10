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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
		final List<Integer> readBytesList = Arrays.asList(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384);
		final List<Integer> reads = Arrays.asList(1000000, 2500000, 5000000, 7500000, 10000000, 25000000, 50000000, 75000000, 100000000);
		
		try {
			generateTestData();
			
			for(final int readBytes : readBytesList) {
				System.out.println("Read requests\tTime random\tTime memory: " + readBytes);
				
				for(final int readRequsts : reads) {
					final Stopwatch stopwatch = Stopwatch.createStarted();
	
					for(int i = 0; i < RETRY; i++) {
						readDataRandom(readRequsts, readBytes);
					}
					
					final int timeRandom = (int) (stopwatch.elapsed().toMillis() / RETRY);
					
					stopwatch.reset();
					stopwatch.start();
					
					for(int i = 0; i < RETRY; i++) {
						readDataMemoryMapped(readRequsts, readBytes);
					}
					
					final int timeMemory = (int) (stopwatch.elapsed().toMillis() / RETRY);
	
					System.out.printf("%d\t%d\t%d\n", readRequsts, timeRandom, timeMemory);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} 
	}

	protected void readDataMemoryMapped(final int readRequsts, final int readBytes) 
			throws Exception {
	
		int nextPosition = 0;
		try {
			final byte[] buffer = new byte[readBytes];
			
			for(int i = 0; i < readRequsts; i++) {
				nextPosition = getNextPosition(readBytes);
				mappedByteBuffer.position(nextPosition);
				mappedByteBuffer.get(buffer, 0, readBytes);
			}
			
		} catch(IllegalArgumentException e) {
			e.printStackTrace();
			System.out.println("Exception by setting to: " + nextPosition);
		}
	}

	protected void readDataRandom(final int readRequsts, final int readBytes) throws Exception {
		
		final byte[] buffer = new byte[readBytes];

		for(int i = 0; i < readRequsts; i++) {
			raf.seek(getNextPosition(readBytes));
			raf.readFully(buffer, 0, readBytes);
		}
	}

	protected int getNextPosition(final int bytesToRead) {
		return Math.abs(random.nextInt() % (FILESIZE - bytesToRead));
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

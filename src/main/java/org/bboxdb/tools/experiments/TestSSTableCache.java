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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.experiments.tuplestore.SSTableTupleStore;
import org.bboxdb.tools.generator.SyntheticDataGenerator;
import org.bboxdb.util.CloseableHelper;
import org.bboxdb.util.io.FileUtil;

import com.google.common.base.Stopwatch;

public class TestSSTableCache implements Runnable {

	/**
	 * The tuple store
	 */
	protected SSTableTupleStore tupleStore = null;
	
	/**
	 * The amount of tuples
	 */
	public final static int TUPLES = 1000000;
	
	/**
	 * Tuple length
	 */
	public final static int TUPLE_LENGTH = 1000;
	
	/** 
	 * The retry counter
	 */
	public final static int RETRY = 3;

	/**
	 * The storage directory
	 */
	private File dir;

	public TestSSTableCache(final File dir) throws Exception {
		this.dir = dir;
	}

	@Override
	public void run() {
		
		final List<Integer> keyCacheElements = Arrays.asList(0, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000);
		System.out.println("#Cache size\tRead");
		
		generateDataset();
		
		for(final int cacheSize : keyCacheElements) {
			
			try {				
				BBoxDBConfigurationManager.getConfiguration().setSstableKeyCacheEntries(cacheSize);

				tupleStore = new SSTableTupleStore(dir);
				tupleStore.open();

				long timeRead = 0;
				
				for(int i = 0; i < RETRY; i++) {				
					timeRead += readTuples();
				}
				
				System.out.format("%d\t%d\n", cacheSize, timeRead / RETRY);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				CloseableHelper.closeWithoutException(tupleStore, CloseableHelper.PRINT_EXCEPTION_ON_STDERR);
				tupleStore = null;
			}
		}
	}

	/**
	 * Generate a new dataset
	 */
	protected void generateDataset() {
		try {
			tupleStore = new SSTableTupleStore(dir);
			tupleStore.open();
			final String data = SyntheticDataGenerator.getRandomString(TUPLE_LENGTH);
			writeTuples(data);
		} catch (Exception e) {
			System.err.println("Got an exception while creating dataset: " + e);
			System.exit(-1);
		} finally {
			CloseableHelper.closeWithoutException(tupleStore, CloseableHelper.PRINT_EXCEPTION_ON_STDERR);
			tupleStore = null;
		}
	}

	/**
	 * Write the tuples
	 * @param data 
	 * @return 
	 * @throws IOException 
	 */
	protected long writeTuples(final String data) throws Exception {
		System.out.println("# Writing Tuples");
		
		final Stopwatch stopwatch = Stopwatch.createStarted();
		
		for(int i = 0; i < TUPLES; i++) {
			final Tuple tuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, data.getBytes());
			tupleStore.writeTuple(tuple);
		}
		
		return stopwatch.elapsed(TimeUnit.MILLISECONDS);
	}

	/**
	 * Read the tuples
	 * @return 
	 * @throws IOException 
	 */
	protected long readTuples() throws Exception {
		System.out.println("# Reading Tuples");
		final Stopwatch stopwatch = Stopwatch.createStarted();

		for(int i = 0; i < TUPLES; i++) {
			tupleStore.readTuple(Integer.toString(i));
		}
		
		return stopwatch.elapsed(TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Main * Main * Main
	 * @throws IOException 
	 */
	public static void main(final String[] args) throws Exception {
		// Check parameter
		if(args.length != 1) {
			System.err.println("Usage: programm <dir>");
			System.exit(-1);
		}
		
		final String dirName = Objects.requireNonNull(args[0]);
		
		final File dir = new File(dirName);
		if(dir.exists()) {
			System.err.println("Dir already exists, please remove");
			System.exit(-1);
		}
		
		// Delete database on exit
		FileUtil.deleteDirOnExit(dir.toPath());

		final TestSSTableCache testSplit = new TestSSTableCache(dir);
		testSplit.run();
	}

}

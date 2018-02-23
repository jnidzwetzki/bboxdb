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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.io.FileUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.experiments.tuplestore.SSTableTupleStore;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.generator.SyntheticDataGenerator;

import com.google.common.base.Stopwatch;

public class TestSSTableBloomFilter implements Runnable {

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

	public TestSSTableBloomFilter(final File dir) throws Exception {
		this.dir = dir;
	}

	@Override
	public void run() {
		
		System.out.println("#Memtable size\tRead sequence\tRead random\t");

		final List<Integer> memtableSizes = Arrays.asList(1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000);
		
		for(final int memtableSize : memtableSizes) {
		
			BBoxDBConfigurationManager.getConfiguration().setMemtableEntriesMax(memtableSize);
	
			// Delete old data
			FileUtil.deleteRecursive(dir.toPath());
			dir.mkdirs();
			
			generateDataset();
			
			SSTableTupleStore tupleStore = null;
			
			try {			
	
				tupleStore = new SSTableTupleStore(dir);
				tupleStore.open();
	
				long timeReadSequence = 0;
				long timeReadRandom = 0;
	
				for(int i = 0; i < RETRY; i++) {				
					timeReadRandom += readTuplesRandom(tupleStore);
				}
				
				System.out.format("%d\t%d\t%d\n", memtableSize, timeReadSequence / RETRY, timeReadRandom / RETRY);
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
		SSTableTupleStore tupleStore = null;
		try {
			tupleStore = new SSTableTupleStore(dir);
			tupleStore.open();
			final String data = SyntheticDataGenerator.getRandomString(TUPLE_LENGTH);
			writeTuples(data, tupleStore);
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
	 * @param tupleStore 
	 * @return 
	 * @throws IOException 
	 */
	protected long writeTuples(final String data, SSTableTupleStore tupleStore) throws Exception {
		System.out.println("# Writing Tuples");
		
		final Stopwatch stopwatch = Stopwatch.createStarted();
		
		for(int i = 0; i < TUPLES; i++) {
			final Tuple tuple = new Tuple(Integer.toString(i), BoundingBox.FULL_SPACE, data.getBytes());
			tupleStore.writeTuple(tuple);
		}
		
		return stopwatch.elapsed(TimeUnit.MILLISECONDS);
	}

	/**
	 * Read the tuples
	 * @param tupleStore 
	 * @return 
	 * @throws IOException 
	 */
	protected long readTuplesRandom(final SSTableTupleStore tupleStore) throws Exception {
		System.out.println("# Reading Tuples random");
		final Stopwatch stopwatch = Stopwatch.createStarted();
		final Random random = new Random();

		for(int i = 0; i < TUPLES; i++) {
			tupleStore.readTuple(Integer.toString(random.nextInt(TUPLES)));
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

		final TestSSTableBloomFilter testSplit = new TestSSTableBloomFilter(dir);
		testSplit.run();
	}

}

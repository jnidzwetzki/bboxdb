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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.io.FileUtil;
import org.bboxdb.experiments.tuplestore.TupleStore;
import org.bboxdb.experiments.tuplestore.TupleStoreFactory;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.generator.SyntheticDataGenerator;

import com.google.common.base.Stopwatch;

public class TestRWPerformance implements Runnable {

	/**
	 * The tuple store
	 */
	protected TupleStore tupleStore = null;
	
	/**
	 * The amount of tuples
	 */
	public final static int TUPLES = 100000;
	
	/** 
	 * The retry counter
	 */
	public final static int RETRY = 3;

	/**
	 * The name of the adapter
	 */
	private String adapterName;

	/**
	 * The storage directory
	 */
	private File dir;

	public TestRWPerformance(final String adapterName, final File dir) throws Exception {
		this.adapterName = adapterName;
		this.dir = dir;
		System.out.println("#Using backend: " + adapterName);
	}

	@Override
	public void run() {
		
		final List<Integer> dataSizes = Arrays.asList(1024, 5120, 10240, 51200, 102400);
		System.out.println("#Size\tWrite\tRead");
		
		for(final int dataSize : dataSizes) {
			
			try {					
				long timeRead = 0;
				long timeWrite = 0;
				
				final String data = SyntheticDataGenerator.getRandomString(dataSize);
	
				for(int i = 0; i < RETRY; i++) {
					FileUtil.deleteRecursive(dir.toPath());
					dir.mkdirs();
					tupleStore = TupleStoreFactory.getTupleStore(adapterName, dir);
					
					timeWrite += writeTuples(data);
					timeRead += readTuples();
				}
				
				System.out.format("%d\t%d\t%d\n", dataSize, timeWrite / RETRY, timeRead / RETRY);
			} catch (Exception e) {
				System.out.println("Got exception: " + e);
			} finally {
				CloseableHelper.closeWithoutException(tupleStore, CloseableHelper.PRINT_EXCEPTION_ON_STDERR);
				tupleStore = null;
			}
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
		tupleStore.open();
		
		final Stopwatch stopwatch = Stopwatch.createStarted();
		
		for(int i = 0; i < TUPLES; i++) {
			final Tuple tuple = new Tuple(Integer.toString(i), BoundingBox.EMPTY_BOX, data.getBytes());
			tupleStore.writeTuple(tuple);
		}
		
		tupleStore.close();
		return stopwatch.elapsed(TimeUnit.MILLISECONDS);
	}

	/**
	 * Read the tuples
	 * @return 
	 * @throws IOException 
	 */
	protected long readTuples() throws Exception {
		System.out.println("# Reading Tuples");
		tupleStore.open();
		final Stopwatch stopwatch = Stopwatch.createStarted();

		for(int i = 0; i < TUPLES; i++) {
			tupleStore.readTuple(Integer.toString(i));
		}
		
		tupleStore.close();
		return stopwatch.elapsed(TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Main * Main * Main
	 * @throws IOException 
	 */
	public static void main(final String[] args) throws Exception {
		// Check parameter
		if(args.length != 2) {
			System.err.println("Usage: programm <adapter> <dir>");
			System.exit(-1);
		}
		
		final String adapter = Objects.requireNonNull(args[0]);
		final String dirName = Objects.requireNonNull(args[1]);
		
		if(! TupleStoreFactory.ALL_STORES.contains(adapter)) {
			System.err.println("Unknown adapter: " + adapter);
			
			final String adapterList = TupleStoreFactory.ALL_STORES
					.stream()
					.collect(Collectors.joining(",", "[", "]"));
			
			System.err.println("Known adapter: " + adapterList);
			System.exit(-1);
		}
		
		final File dir = new File(dirName);
		if(dir.exists()) {
			System.err.println("Dir already exists, please remove");
			System.exit(-1);
		}
		
		// Delete database on exit
		FileUtil.deleteDirOnExit(dir.toPath());

		final TestRWPerformance testSplit = new TestRWPerformance(adapter, dir);
		testSplit.run();
	}

}

/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.experiments.conference;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.bboxdb.commons.io.FileUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreAquirer;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.tools.TupleFileReader;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class TestSpatialIndex implements Runnable, Closeable {

	/**
	 * The file to import
	 */
	private final Map<String, String> filesAndFormats;

	/**
	 * The samples that are used for the query
	 */
	private final List<Hyperrectangle> samples;

	/**
	 * The tuple counter
	 */
	private final AtomicLong tupleCounter;

	/**
	 * The storage manager
	 */
	private TupleStoreManager storageManager;

	/**
	 * The storage registry
	 */
	private final TupleStoreManagerRegistry storageRegistry;

	/**
	 * The destination table
	 */
	private final static String TABLENAME = "testgroup_testtable";

	/**
	 * The number of needed samples
	 */
	private final static int NEEDED_SAMPLES = 100;


	public TestSpatialIndex(final File tmpDir, final Map<String, String> filesAndFormats) throws Exception {

		this.filesAndFormats = filesAndFormats;
		this.samples = new ArrayList<>();

		// Setup database dir
		tmpDir.mkdirs();
		FileUtil.deleteDirOnExit(tmpDir.toPath());
		this.tupleCounter = new AtomicLong(0);

		// Set the storage path
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		configuration.setStorageDirectories(Arrays.asList(tmpDir.getAbsoluteFile().toString()));

		this.storageRegistry = new TupleStoreManagerRegistry(configuration);
		storageRegistry.init();

		final TupleStoreName tupleStoreName = new TupleStoreName(TABLENAME);

		final TupleStoreConfiguration config = TupleStoreConfigurationBuilder
				.create()
				.allowDuplicates(false)
				.build();

		storageManager = storageRegistry.createTable(tupleStoreName, config);
	}

	@Override
	public void run() {
		importData();
		queryDataWithIndex();
		queryDataWithoutIndex();
	}

	/**
	 * Query data with spatial index
	 */
	private void queryDataWithIndex() {

		try(final TupleStoreAquirer tupleStoreAquirer = new TupleStoreAquirer(storageManager)) {

			final Stopwatch stopwatchAll = Stopwatch.createStarted();

			for(final Hyperrectangle sample : samples) {
				final Stopwatch stopwatchExperiment = Stopwatch.createStarted();

				long results = 0;

				for(final ReadOnlyTupleStore storge : tupleStoreAquirer.getTupleStores()) {
					final Iterator<Tuple> resultIterator = storge.getAllTuplesInBoundingBox(sample);
					final ArrayList<Tuple> resultTuples = Lists.newArrayList(resultIterator);
					results = results + resultTuples.size();
				}

				final long elapsed = stopwatchExperiment.elapsed(TimeUnit.MILLISECONDS);

				System.out.println("Query with index done in (ms) / results: "
					+ elapsed + " / " + results);
			}

			final long elapsed = stopwatchAll.elapsed(TimeUnit.MILLISECONDS);
			System.out.println("All queries queries with index done in (ms) " + elapsed);
			System.out.println("Average time (ms) " + elapsed / samples.size());

		} catch (StorageManagerException e) {
			System.err.println("Got an Exception during query");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Query data without spatial index
	 */
	private void queryDataWithoutIndex() {

		try {
			final Stopwatch stopwatchAll = Stopwatch.createStarted();

			for(final Hyperrectangle sample : samples) {


				final Stopwatch stopwatch = Stopwatch.createStarted();

				long results = 0;

				for(long key = 0; key < tupleCounter.get(); key++) {
					final String keyString = Long.toString(key);
					final List<Tuple> resultTuples = storageManager.get(keyString);

					for(final Tuple tuple : resultTuples) {
						if(sample.intersects(tuple.getBoundingBox())) {
							results++;
						}
					}
				}

				final long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

				System.out.println("Query without index done in (ms) / results: "
						+ elapsed + " / " + results);
			}

			final long elapsed = stopwatchAll.elapsed(TimeUnit.MILLISECONDS);
			System.out.println("All queries queries without index done in (ms) " + elapsed);
			System.out.println("Average time (ms) " + elapsed / samples.size());

		} catch (StorageManagerException e) {
			System.err.println("Got an Exception during query");
			e.printStackTrace();
			System.exit(-1);
		}

	}

	private void importData() {
		System.out.println("Importing data");

		final Stopwatch stopwatch = Stopwatch.createStarted();

		for(final Entry<String, String> elements : filesAndFormats.entrySet()) {
			final String filename = elements.getKey();
			final String format = elements.getValue();

			System.out.println("Processing file: " + filename);
			final TupleFileReader tupleFile = new TupleFileReader(filename, format);

			tupleFile.addTupleListener(t -> {
				final long counter = tupleCounter.incrementAndGet();
				insertTuple(t, counter);

				// Take sample if needed or replace randomly
				if(samples.size() < NEEDED_SAMPLES) {
					samples.add(t.getBoundingBox());
				} else {
					final ThreadLocalRandom random = ThreadLocalRandom.current();

					if(random.nextInt(100) < 1) {
						samples.set(random.nextInt(NEEDED_SAMPLES), t.getBoundingBox());
					}
				}
			});

			try {
				tupleFile.processFile();
			} catch (Exception e) {
				System.err.println("Got an Exception during experiment");
				e.printStackTrace();
				System.exit(-1);
			}
		}

		System.out.println("Import done in (ms) " + stopwatch.elapsed(TimeUnit.MILLISECONDS)
			+ " / " + tupleCounter.get());
	}

	private void insertTuple(final Tuple tuple, final long key) {
		try {
			final Tuple createdTuple = new Tuple(Long.toString(key), tuple.getBoundingBox(), tuple.getDataBytes());
			storageManager.put(createdTuple);
		} catch (Exception e) {
			System.err.println("Got an Exception during experiment");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			storageRegistry.deleteTable(new TupleStoreName(TABLENAME));
			storageRegistry.shutdown();
		} catch (StorageManagerException e) {
			throw new IOException();
		}
	}


	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws Exception {

		// Check parameter
		if(args.length < 2) {
			System.err.println("Usage: programm <tmpdir> <filename1:format1> <filenameN:formatN> <filenameN:formatN>");
			System.exit(-1);
		}

		final String tmpDir = args[0];
		final File dir = new File(tmpDir);

		if(! dir.exists()) {
			System.err.format("Dir %s does not exist, exiting%n", tmpDir);
			System.exit(-1);
		}

		final Map<String, String> filesAndFormats = new HashMap<>();

		for(int pos = 1; pos < args.length; pos++) {

			final String element = args[pos];

			if(! element.contains(":")) {
				System.err.println("Element does not contain format specifier: " + element);
				System.exit(-1);
			}

			final String[] splitFile = element.split(":");

			if(splitFile.length != 2) {
				System.err.println("Unable to get two elements after format split: " + element);
				System.exit(-1);
			}

			filesAndFormats.put(splitFile[0], splitFile[1]);
		}

		final TestSpatialIndex testSplit = new TestSpatialIndex(dir, filesAndFormats);
		testSplit.run();
		testSplit.close();
	}

}

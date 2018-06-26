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
package org.bboxdb.experiments.conference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.network.client.tools.IndexedTupleUpdateHelper;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class TestIndexBasedUpdate implements Runnable {

	/**
	 * The endpoint name
	 */
	private String endpoint;

	/**
	 * The cluster name
	 */
	private String cluster;

	/**
	 * The tablename to query
	 */
	private final String tablename;

	/**
	 * The number of experiment retries
	 */
	private final static int RETRIES = 5;

	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;

	/**
	 * The number of queries to execute
	 */
	private int queries;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TestIndexBasedUpdate.class);


	public TestIndexBasedUpdate(final String endpoint, final String cluster, final String tablename,
			final int queries) {

		this.endpoint = endpoint;
		this.cluster = cluster;
		this.tablename = tablename;
		this.queries = queries;
		this.pendingFutures = new FixedSizeFutureStore(1000);
	}

	@Override
	public void run() {
		try {
			System.out.println("Connecting to BBoxDB cluster");
			final BBoxDBCluster bboxDBConnection = new BBoxDBCluster(endpoint, cluster);

			if(! bboxDBConnection.connect()) {
				System.err.println("Unable to connect to the BBoxDB cluster, exiting");
				System.exit(-1);
			}

			ZookeeperClient zookeeperClient = bboxDBConnection.getZookeeperClient();
			final DistributionGroupAdapter adapter = zookeeperClient.getDistributionGroupAdapter();
			String distributionGroup = new TupleStoreName(tablename).getDistributionGroup();
			final DistributionGroupConfiguration configuration = adapter.getDistributionGroupConfiguration(distributionGroup);
			final int dimensions = configuration.getDimensions();

			final List<Integer> worker = Arrays.asList(1, 2, 3, 5, 10, 20, 50);

			// Updates without index
			System.out.format("# Running n queries and p parallel worker (non index) %n");
			worker.forEach(w -> runExperiment(bboxDBConnection, w, dimensions, false));

			// Index based updates
			System.out.format("# Running n queries and p parallel worker (Index) %n");
			worker.forEach(w -> runExperiment(bboxDBConnection, w, dimensions, true));

		} catch (Exception e) {
			logger.error("Got exception while performing experiment", e);
		}
	}

	/**
	 * Run the experiment with the max dimension size
	 * @param bboxDBConnection
	 * @throws BBoxDBException
	 * @throws IOException
	 */
	private void runExperiment(final BBoxDBCluster bboxDBConnection, final int worker,
			final int dimensions, final boolean useIndex) {

		try {
			long totalElapsedTime = 0;
			for(int execution = 0; execution < RETRIES; execution++) {
				final Stopwatch stopwatch = Stopwatch.createStarted();
				executeQueriesOnIndex(worker, bboxDBConnection, dimensions, useIndex);
				final long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
				System.out.println("#" + elapsedTime);
				totalElapsedTime += elapsedTime;
			}

			System.out.format("%f\t%f\t%f%n", queries, worker, totalElapsedTime / RETRIES);
		} catch (Exception e) {
			System.err.println("Got exception while executing experiment: " + e);
			System.exit(-1);
		}
	}

	/**
	 * Execute the experiment queries on index
	 * @param worker
	 * @param bboxDBConnection
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 * @throws RejectedException
	 */
	private void executeQueriesOnIndex(final int worker, final BBoxDBCluster bboxDBConnection,
			final int dimensions, final boolean useIndex)
					throws BBoxDBException, InterruptedException, RejectedException {

		final List<Thread> threads = new ArrayList<>();

		for(int threadNumber = 0; threadNumber < worker; threadNumber++) {
			Runnable runable = null;

			if(useIndex) {
				runable = getNewRunableIndex(bboxDBConnection, dimensions);
			} else {
				runable = getNewRunable(bboxDBConnection, dimensions);
			}

			final Thread thread = new Thread(runable);
			thread.start();
			threads.add(thread);
		}

		for(final Thread thread : threads) {
			thread.join();
		}

		pendingFutures.waitForCompletion();
	}

	/**
	 * Create a new runable
	 * @param bboxDBConnection
	 * @param dimensions
	 * @return
	 */
	private Runnable getNewRunable(final BBoxDBCluster bboxDBConnection, final int dimensions) {
		final Runnable run = () -> {
			try {
				for(int i = 0; i < queries; i++) {
					final double randomDouble = ThreadLocalRandom.current().nextDouble(1000);
					final String key = Double.toString(randomDouble);

					// Determine query bounding box
					final List<DoubleInterval> bboxIntervals = new ArrayList<>();
					for(int dimension = 0; dimension < dimensions; dimension++) {
						final double d1 = ThreadLocalRandom.current().nextDouble(1000);
						final double d2 = ThreadLocalRandom.current().nextDouble(1000);

						final double coordinateLow = d1;
						final double coordinateHigh = d1 + d2;

						final DoubleInterval doubleInterval = new DoubleInterval(coordinateLow, coordinateHigh);
						bboxIntervals.add(doubleInterval);
					}

					final Hyperrectangle box = new Hyperrectangle(bboxIntervals);

					final TupleListFuture keyQuery = bboxDBConnection.queryKey(tablename, key);
					pendingFutures.put(keyQuery);

					final EmptyResultFuture deleteQuery = bboxDBConnection.deleteTuple(tablename, key, System.currentTimeMillis(), box);
					pendingFutures.put(deleteQuery);

					final Tuple tuple = new Tuple(key, box, "".getBytes());

					final EmptyResultFuture updateRequest = bboxDBConnection.insertTuple(tablename, tuple);
					pendingFutures.put(updateRequest);
				}
			} catch (Exception e) {
				logger.error("Got an exception in update thread", e);
			}
		};

		return run;
	}

	/**
	 * Create a new runable
	 * @param bboxDBConnection
	 * @param dimensions
	 * @return
	 */
	private Runnable getNewRunableIndex(final BBoxDBCluster bboxDBConnection, final int dimensions) {
		final Runnable run = () -> {
			try {
				final IndexedTupleUpdateHelper updateHelper = new IndexedTupleUpdateHelper(bboxDBConnection);

				for(int i = 0; i < queries; i++) {
					final double randomDouble = ThreadLocalRandom.current().nextDouble(1000);
					final String key = Double.toString(randomDouble);
					updateHelper.getOldIndexEntry(tablename, key);

					// Determine query bounding box
					final List<DoubleInterval> bboxIntervals = new ArrayList<>();
					for(int dimension = 0; dimension < dimensions; dimension++) {
						final double d1 = ThreadLocalRandom.current().nextDouble(1000);
						final double d2 = ThreadLocalRandom.current().nextDouble(1000);

						final double coordinateLow = d1;
						final double coordinateHigh = d1 + d2;

						final DoubleInterval doubleInterval = new DoubleInterval(coordinateLow, coordinateHigh);
						bboxIntervals.add(doubleInterval);
					}

					final Hyperrectangle box = new Hyperrectangle(bboxIntervals);

					final Tuple tuple = new Tuple(key, box, "".getBytes());
					final EmptyResultFuture updateRequest = updateHelper.handleTupleUpdate(tablename, tuple);
					pendingFutures.put(updateRequest);
				}
			} catch (Exception e) {
				logger.error("Got an exception in update thread", e);
			}
		};

		return run;
	}

	/**
	 * Main * Main * Main
	 */
	public static void main(final String[] args) throws Exception {

		// Check parameter
		if(args.length != 4) {
			System.err.println("Usage: programm <endpoint> <cluster> <table> <queries>");
			System.exit(-1);
		}

		final String endpoint = Objects.requireNonNull(args[0]);
		final String cluster = Objects.requireNonNull(args[1]);
		final String table = Objects.requireNonNull(args[2]);
		final String queriesString = Objects.requireNonNull(args[3]);

		final int queries = MathUtil.tryParseInt(queriesString, () -> "Unable to parse: " + queriesString);

		final TestIndexBasedUpdate experiment = new TestIndexBasedUpdate(endpoint, cluster, table, queries);

		experiment.run();
	}

}

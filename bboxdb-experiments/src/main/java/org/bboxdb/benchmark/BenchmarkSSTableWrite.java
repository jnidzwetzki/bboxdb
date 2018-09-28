package org.bboxdb.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;

import com.google.common.base.Stopwatch;

public class BenchmarkSSTableWrite {

	protected final static TupleStoreName TEST_RELATION = new TupleStoreName("testgroup1_abc");

	public static void main(String[] args) throws StorageManagerException, RejectedException, InterruptedException, BBoxDBException {

		final List<Long> elapsedBenchmarks = new ArrayList<>();

		final TupleStoreManagerRegistry storageRegistry = new TupleStoreManagerRegistry();
		storageRegistry.init();

		// Delete the old table
		storageRegistry.deleteTable(TEST_RELATION);

		// Create a new table
		final TupleStoreConfiguration tupleStoreConfiguration = TupleStoreConfigurationBuilder.create().build();
		storageRegistry.createTable(TEST_RELATION, tupleStoreConfiguration);

		// Assure table is created successfully
		final TupleStoreManager storageManager = storageRegistry.getTupleStoreManager(TEST_RELATION);

		for(int iter = 0; iter < 100; iter++) {
			final Stopwatch watch = Stopwatch.createStarted();

			int MAX_TUPLES = 100000;
			int SPECIAL_TUPLE = MAX_TUPLES / 2;

			for(int i = 0; i < MAX_TUPLES; i++) {
				final double d1 = ThreadLocalRandom.current().nextDouble();
				final double d2 = ThreadLocalRandom.current().nextDouble();
				final double d3 = ThreadLocalRandom.current().nextDouble();

				final Hyperrectangle hyperrectangle = new Hyperrectangle(d1, d1 + 10.0, d2, d2 + 10.0, d3, d3 + 10.0);

				final Tuple createdTuple = new Tuple(Integer.toString(i), hyperrectangle, Integer.toString(i).getBytes());
				storageManager.put(createdTuple);

				if(i == SPECIAL_TUPLE) {
					storageManager.delete(Integer.toString(SPECIAL_TUPLE), createdTuple.getVersionTimestamp() + 1);
				}
			}

			final long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
			elapsedBenchmarks.add(elapsed);

			System.out.format("Iteartion %d, Elapsed: %d%n", iter, elapsed);
		}

		final long max = elapsedBenchmarks.stream().mapToLong(l -> l).max().orElse(0);
		final long min = elapsedBenchmarks.stream().mapToLong(l -> l).min().orElse(0);
		final double avg = elapsedBenchmarks.stream().mapToLong(l -> l).average().orElse(0);

		System.out.format("Max %d, Min %d, Avg %f%n", max, min, avg);
	}

}

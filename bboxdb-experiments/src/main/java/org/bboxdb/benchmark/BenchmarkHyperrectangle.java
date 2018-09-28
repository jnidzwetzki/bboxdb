package org.bboxdb.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.math.Hyperrectangle;

import com.google.common.base.Stopwatch;

public class BenchmarkHyperrectangle {

	public static void main(String[] args) {
		final List<Hyperrectangle> rectangles = new ArrayList<>();

		for(long i = 0; i < 10_000_000; i++) {
			final double d1 = ThreadLocalRandom.current().nextDouble();
			final double d2 = ThreadLocalRandom.current().nextDouble();
			final double d3 = ThreadLocalRandom.current().nextDouble();

			final Hyperrectangle hyperrectangle = new Hyperrectangle(d1, d1 + 10.0, d2, d2 + 10.0, d3, d3 + 10.0);
			rectangles.add(hyperrectangle);
		}


		final List<Long> elapsedBenchmarks = new ArrayList<>();

		for(int i = 0; i < 100; i++) {
		final Stopwatch watch = Stopwatch.createStarted();
			Hyperrectangle.getCoveringBox(rectangles);

			final long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
			elapsedBenchmarks.add(elapsed);

			System.out.format("Iteartion %d, Elapsed: %d%n", i, elapsed);
		}

		final long max = elapsedBenchmarks.stream().mapToLong(l -> l).max().orElse(0);
		final long min = elapsedBenchmarks.stream().mapToLong(l -> l).min().orElse(0);
		final double avg = elapsedBenchmarks.stream().mapToLong(l -> l).average().orElse(0);

		System.out.format("Max %d, Min %d, Avg %f%n", max, min, avg);
	}

}

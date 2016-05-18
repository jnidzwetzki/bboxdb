package de.fernunihagen.dna.jkn.scalephant.performance;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.client.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class BenchmarkKeyQueryPerformance extends AbstractBenchmark {

	/**
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);

	/**
	 * The amount of tuples to insert
	 */
	protected final int tuplesToInsert;

	/**
	 * The name of the table
	 */
	protected String mytable = "2_mygroup3_testdata";

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BenchmarkKeyQueryPerformance.class);

	
	public BenchmarkKeyQueryPerformance(final int tuplesToInsert) {
		super();
		this.tuplesToInsert = tuplesToInsert;
	}

	
	@Override
	protected void prepare() throws Exception {
		super.prepare();
		
		// Remove old data
		final ClientOperationFuture result = scalephantClient.deleteTable(mytable);
		result.get();		
		
		logger.info("Inserting " + tuplesToInsert + " tuples");
	
		// Insert the tuples
		for(; insertedTuples.get() < tuplesToInsert; insertedTuples.incrementAndGet()) {
			scalephantClient.insertTuple(mytable, new Tuple(Integer.toString(insertedTuples.get()), BoundingBox.EMPTY_BOX, "abcdef".getBytes()));
		}
		
		// Wait for requests to settle
		logger.info("Wait for insert requests to settle");
		while(scalephantClient.getInFlightCalls() != 0) {
			logger.info(scalephantClient.getInFlightCalls() + " are pending");
			Thread.sleep(1000);
		}
		logger.info("All insert requests are settled");
	}
	
	@Override
	protected void startBenchmarkTimer() {
		// Set the benchmark time
		startTime = System.currentTimeMillis();
		System.out.println("#Iteration\tTime");
	}
	
	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException {
	
		for(int i = 0; i < 100; i++) {
			final long start = System.nanoTime();
			final ClientOperationFuture result = scalephantClient.queryKey(mytable, Integer.toString(40));
			
			// Wait for request to settle
			final Object queryResult = result.get();
			if(! (queryResult instanceof Tuple)) {
				logger.warn("Query failed");
			}
			
			final long end = System.nanoTime();
			System.out.println(i + "\t" + (end-start));
			
			Thread.sleep(1000);
		}
	}
	
	@Override
	protected DataTable getDataTable() {
		// We don't need a data model for this benchmark
		return null;
	}

	/* ====================================================
	 * Main
	 * ====================================================
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final int[] tupleAmount = {10000, 50000, 100000, 50000, 1000000, 500000};
		
		for(final int tuples : tupleAmount) {
			final BenchmarkKeyQueryPerformance benchmarkInsertPerformance = new BenchmarkKeyQueryPerformance(tuples);
			benchmarkInsertPerformance.run();
		}
	}
	
}

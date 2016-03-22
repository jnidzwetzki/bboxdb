package de.fernunihagen.dna.jkn.scalephant.performance;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import de.fernunihagen.dna.jkn.scalephant.network.client.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class BenchmarkInsertPerformance extends AbstractBenchmark {

	/**
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);

	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException {
		// A 2 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
		final String mytable = "2_mygroup3_testdata";
		
		// Number of tuples
		final int tuples = 5000000;
				
		// Remove old data
		final ClientOperationFuture result = scalephantClient.deleteTable(mytable);
		result.get();		
	
		// Insert the tuples
		for(; insertedTuples.get() < tuples; insertedTuples.incrementAndGet()) {
			scalephantClient.insertTuple(mytable, new Tuple(Integer.toString(insertedTuples.get()), BoundingBox.EMPTY_BOX, "abcdef".getBytes()));
		}
	}

	@Override
	protected DataTable getDataTable() {
		return new DataTable() {
			
			protected int lastInsertedTuples = 0;
			protected int diff = 0;
			
			@Override
			public String getValueForColum(short colum) {
				switch (colum) {
				
				// Total amount of inserted tuples
				case 0:
					final int tuples = insertedTuples.get();
					diff = tuples - lastInsertedTuples;
					lastInsertedTuples = tuples;
					return Integer.toString(tuples);
					
				// Diff amount of inserted tuples
				case 1:
					return Integer.toString(diff);

				default:
					return "-----";
				}
			}
			
			@Override
			public String getTableHeader() {
				return "#Time\tTuples\tTuples_per_sec";
			}
			
			@Override
			public short getColumns() {
				return 2;
			}
		};
	}	
	
	/* ====================================================
	 * Main
	 * ====================================================
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final BenchmarkInsertPerformance benchmarkInsertPerformance = new BenchmarkInsertPerformance();
		benchmarkInsertPerformance.run();
	}
	
}

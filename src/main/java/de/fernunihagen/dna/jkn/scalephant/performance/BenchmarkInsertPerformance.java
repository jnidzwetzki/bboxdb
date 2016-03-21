package de.fernunihagen.dna.jkn.scalephant.performance;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;


public class BenchmarkInsertPerformance {

	/**
	 * Unix time of the benchmark start
	 */
	protected static long startTime = 0;
	
	public static void main(String[] args) {
		// A 2 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
		final String mytable = "2_mygroup3_testdata";
		
		// Number of tuples
		final int tuples = 5000000;
		
		final AtomicInteger insertedTuples = new AtomicInteger(0);
		
		// Connect to the server
		final ScalephantClient scalephantClient = new ScalephantClient("127.0.0.1");
		scalephantClient.connect();
		
		if(! scalephantClient.isConnected()) {
			System.out.println("Connection could not be established");
			System.exit(-1);
		}
		
		// Remove old data
		scalephantClient.deleteTable(mytable);
		
		startTime = System.currentTimeMillis();
		System.out.println("#Time\tTuples");
		
		// Dump performance info every second
		final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);
		executorService.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				System.out.println(System.currentTimeMillis() - startTime + "\t" + insertedTuples.get());
			}
			
		}, 0, 1, TimeUnit.SECONDS);
	
		// Insert the tuples
		for(; insertedTuples.get() < tuples; insertedTuples.incrementAndGet()) {
			scalephantClient.insertTuple(mytable, new Tuple(Integer.toString(insertedTuples.get()), BoundingBox.EMPTY_BOX, "abcdef".getBytes()));
		}
		
		// Disconnect from server and shutdown the statistics thread
		scalephantClient.disconnect();
		executorService.shutdown();
			
		System.out.println("Done in " + (System.currentTimeMillis() - startTime) + " ms");
	}
	
}

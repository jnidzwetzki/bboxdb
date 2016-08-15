package de.fernunihagen.dna.jkn.scalephant.performance;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.network.client.OperationFuture;
import de.fernunihagen.dna.jkn.scalephant.network.client.Scalephant;
import de.fernunihagen.dna.jkn.scalephant.network.client.ScalephantCluster;

public abstract class AbstractBenchmark implements Runnable {
	
	/**
	 * Unix time of the benchmark start
	 */
	protected long startTime = 0;
	
	/**
	 * The scalephant client
	 */
	protected Scalephant scalephantClient;
	
	/**
	 * The executor service
	 */
	protected ScheduledExecutorService executorService;
	
	/**
	 * Status variable - is the benchmark actvive
	 */
	protected volatile boolean benchmarkActive;
	
	/**
	 * The pending futures
	 */
	protected final List<OperationFuture> pendingFutures;
	
	/**
	 * The amount of pending insert futures
	 */
	protected final static int MAX_PENDING_FUTURES = 5000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(AbstractBenchmark.class);

	public AbstractBenchmark() {
		pendingFutures = new LinkedList<OperationFuture>();
	}
	
	protected void checkForCompletedFutures() {
		if(pendingFutures.size() > MAX_PENDING_FUTURES) {
			
			// Reduce futures
			while(pendingFutures.size() > MAX_PENDING_FUTURES * 0.8) {
				
				// Remove old futures
				final Iterator<OperationFuture> futureIterator = pendingFutures.iterator();
				while(futureIterator.hasNext()) {
					final OperationFuture future = futureIterator.next();
					
					if(future.isDone()) {
						futureIterator.remove();
					}
					
					if(future.isFailed()) {
						logger.error("Failed future detected: " + future);
						futureIterator.remove();
					}
				}

				// Still to much futures? Wait some time
				if(pendingFutures.size() > MAX_PENDING_FUTURES * 0.8) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						// Ignore exception
					}
				}
			}
		}
	}
	
	/**
	 * Run the benchmark
	 */
	@Override
	public void run() {
		try {
			prepare();
			startBenchmarkTimer();
			runBenchmark();
			done();
		} catch (Exception e) {
			logger.error("Got an exception", e);
			System.exit(-1);
		}
	}

	/**
	 * The default prepare method
	 */
	protected void prepare() throws Exception {
		// Set the benchmark flag to active
		benchmarkActive = true;
		
		// Connect to the scalephant cluster
		final Collection<String> zookeeperNodes = ScalephantConfigurationManager.getConfiguration().getZookeepernodes();
		final String clustername = ScalephantConfigurationManager.getConfiguration().getClustername();
		scalephantClient = new ScalephantCluster(zookeeperNodes, clustername);
		scalephantClient.connect();
		
		if(! scalephantClient.isConnected()) {
			throw new Exception("Connection could not be established: " + zookeeperNodes);
		}
		
		executorService = Executors.newScheduledThreadPool(10);		
	}

	/**
	 * Start the benchmark timer
	 */
	protected void startBenchmarkTimer() {
		// Init the data table
		final DataTable dataTable = getDataTable();
		System.out.println(dataTable.getTableHeader());
		
		// Set the benchmark time
		startTime = System.currentTimeMillis();
		
		// Dump performance info every second
		executorService.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				final StringBuffer sb = new StringBuffer();
				sb.append(System.currentTimeMillis() - startTime + "\t");
				
				for(short i = 0; i < dataTable.getColumns(); i++) {
					sb.append(dataTable.getValueForColum(i));
					sb.append("\t");
				}
				
				System.out.println(sb.toString());
			}
			
		}, 0, 1, TimeUnit.SECONDS);
	}
	
	/**
	 * The benchmark
	 */
	protected abstract void runBenchmark() throws Exception;
	
	/**
	 * The default done method
	 */
	protected void done() throws Exception {
		
		// Wait for pending futures
		if(! pendingFutures.isEmpty()) {
			System.out.println("Wait for pending futures to settle: " + pendingFutures.size());
			
			for(final OperationFuture future : pendingFutures) {
				future.waitForAll();
				
				if(future.isFailed()) {
					System.out.println("Got failed future: " + future);
				}
			}
			
			pendingFutures.clear();
		}
		
		// Disconnect from server and shutdown the statistics thread
		scalephantClient.disconnect();
		executorService.shutdown();
			
		System.out.println("Done in " + (System.currentTimeMillis() - startTime) + " ms");
		
		// Set the benchmark flag to finish
		benchmarkActive = false;
	}
	
	/**
	 * Get the data table for the print thread
	 * @return
	 */
	protected abstract DataTable getDataTable();
	
}

/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.converter;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBenchmark implements Runnable {
	
	/**
	 * Unix time of the benchmark start
	 */
	protected long startTime = 0;
	
	/**
	 * The BBoxDB client
	 */
	protected BBoxDB bboxdbClient;
	
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
						
						if(future.isFailed()) {
							logger.error("Failed future detected: " + future);
						}
						
						continue;
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
		
		// Connect to the BBoxDB cluster
		final Collection<String> zookeeperNodes = BBoxDBConfigurationManager.getConfiguration().getZookeepernodes();
		final String clustername = BBoxDBConfigurationManager.getConfiguration().getClustername();
		bboxdbClient = new BBoxDBCluster(zookeeperNodes, clustername);
		bboxdbClient.connect();
		
		if(! bboxdbClient.isConnected()) {
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
		
		if(dataTable == null) {
			System.err.println("Unable to construct data table");
			return;
		}
		
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
		bboxdbClient.disconnect();
		executorService.shutdown();
		
		final long executionTime = System.currentTimeMillis() - startTime;
		System.out.format("Done in %d ms\n", executionTime);
		
		// Set the benchmark flag to finish
		benchmarkActive = false;
	}
	
	/**
	 * Get the data table for the print thread
	 * @return
	 */
	protected abstract DataTable getDataTable();
	
}

/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
package org.bboxdb.tools.network;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchAuTransport implements Runnable {
	
	/**
	 * The data sources
	 *
	 */
	public static class DataSources {
		public final static String API_ENDPOINT_BASE = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/";
		
		public final static String SYDNEYTRAINS = "trains";
		public final static String BUSES = "buses";
		public final static String FERRIES = "ferries";
		public final static String LIGHTRAIL = "lightrail";
		public final static String NSWTRAINS = "nswtrains";
		public final static String REGIOBUSES = "regionbuses";
		public final static String METRO = "metro";
		
		public final static Map<String, String> API_ENDPOINT = new HashMap<>();
		
		static {
			API_ENDPOINT.put(SYDNEYTRAINS, API_ENDPOINT_BASE + "sydneytrains");
			API_ENDPOINT.put(BUSES, API_ENDPOINT_BASE + "buses");
			API_ENDPOINT.put(FERRIES, API_ENDPOINT_BASE + "ferries/sydneyferries");
			API_ENDPOINT.put(LIGHTRAIL, API_ENDPOINT_BASE + "lightrail");
			API_ENDPOINT.put(NSWTRAINS, API_ENDPOINT_BASE + "nswtrains");
			API_ENDPOINT.put(REGIOBUSES, API_ENDPOINT_BASE + "regionbuses");
			API_ENDPOINT.put(METRO, API_ENDPOINT_BASE + "metro");
		}
	}

	/**
	 * The amount of pending insert futures
	 */
	private final static int MAX_PENDING_FUTURES = 1000;
	
	/**
	 * The polling delay
	 */
	private long fetchDelay;
	
	/**
	 * The Auth key
	 */
	private final String authKey;
	
	/**
	 * The connection point
	 */
	private final String connectionPoint;
	
	/**
	 * The cluster name
	 */
	private final String clustername;
	
	/**
	 * The distributionGroup
	 */
	private final String distributionGroup;
	
	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;
	
	/**
	 * The thread pool
	 */
	private final ExecutorService threadPool;
	
	/**
	 * The entities
	 */
	private final String[] entities;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FetchAuTransport.class);
	
	public FetchAuTransport(final String authKey, final String[] entities, final String connectionPoint, 
			final String clustername, final String distributionGroup, final int delay) {
				this.authKey = authKey;
				this.entities = entities;
				this.connectionPoint = connectionPoint;
				this.clustername = clustername;
				this.distributionGroup = distributionGroup;
				this.fetchDelay = TimeUnit.SECONDS.toMillis(delay);
				this.pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES);
				this.threadPool = Executors.newCachedThreadPool();
	}

	@Override
	public void run() {
		try (
	    		final BBoxDB bboxdbClient = new BBoxDBCluster(connectionPoint, clustername);
	        ){
			bboxdbClient.connect();

			for(final String entity: entities) {
				logger.info("Starting fetch thread for {}", entity);
				
				final String urlString = DataSources.API_ENDPOINT.get(entity);
				
				if(urlString == null) {
					logger.error("Unable to determine URL for: " + entity);
					System.exit(-1);
				}
				
				final String table = distributionGroup + "_" + entity;
			
				final FetchRunable runable = new FetchRunable(urlString, authKey, bboxdbClient, 
						table, pendingFutures, fetchDelay);
				
				threadPool.submit(runable);
			}
			
		} catch (Exception e) {
			logger.error("Got an exception", e);
		} finally {
			waitForPendingFutures();
			threadPool.shutdownNow();
		}   
	}
	
	
	/**
	 * Wait for the pending futures
	 */
	private void waitForPendingFutures() {
		try {
			pendingFutures.waitForCompletion();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Main * Main * Main * Main * Main
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		if(args.length != 5) {
			System.err.println("Usage: <Class> <AuthKey> <Entity1:Entity2:EntityN> <Connection Endpoint> <Clustername> <DistributionGroup> <Delay in sec>");
			System.exit(-1);
		}
		
		final String authKey = args[0];
		final String[] entities = args[1].split(":");
		final String connectionPoint = args[2];
		final String clustername = args[3];
		final String distributionGroup = args[4];
		final int delay = MathUtil.tryParseIntOrExit(args[5], () -> "Unable to parse delay value: " 
				+ args[5]);
				
		final FetchAuTransport main = new FetchAuTransport(authKey, entities, connectionPoint, 
				clustername, distributionGroup, delay);
		main.run();
	}
}

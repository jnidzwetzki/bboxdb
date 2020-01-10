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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.GeoJSONTupleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportAuTransport implements Runnable {

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
	private final static Logger logger = LoggerFactory.getLogger(ImportAuTransport.class);
	
	public ImportAuTransport(final String authKey, final String[] entities, final String connectionPoint, 
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
				
				final String urlString = AuTransportSources.API_ENDPOINT.get(entity);
				
				if(urlString == null) {
					logger.error("Unable to determine URL for: " + entity);
					System.exit(-1);
				}
				
				final String table = distributionGroup + "_" + entity;
				
				final Consumer<GeoJsonPolygon> consumer = (polygon) -> {
					final GeoJSONTupleBuilder tupleBuilder = new GeoJSONTupleBuilder();
					final String key = Long.toString(polygon.getId());
					final Tuple tuple = tupleBuilder.buildTuple(key, polygon.toGeoJson());

					try {
						final EmptyResultFuture insertFuture = bboxdbClient.insertTuple(table, tuple);
						pendingFutures.put(insertFuture);
					} catch (BBoxDBException e) {
						logger.error("Got error while inserting tuple");
					}
				};
			
				final FetchRunable runable = new FetchRunable(urlString, authKey, consumer, fetchDelay, entity);
				
				threadPool.submit(runable);
			}
			
			// Wait forever
			threadPool.shutdown();
			threadPool.awaitTermination(999999, TimeUnit.DAYS);
			
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
		
		if(args.length != 6) {
			System.err.println("Usage: <Class> <AuthKey> <Entity1:Entity2:EntityN> <Connection Endpoint> <Clustername> <DistributionGroup> <Delay in sec>");
			System.err.println("Entities: " + AuTransportSources.SUPPORTED_ENTITIES);
			System.exit(-1);
		}
		
		final String authKey = args[0];
		final String[] entities = args[1].split(":");
		final String connectionPoint = args[2];
		final String clustername = args[3];
		final String distributionGroup = args[4];
		final int delay = MathUtil.tryParseIntOrExit(args[5], () -> "Unable to parse delay value: " 
				+ args[5]);
				
		final ImportAuTransport main = new ImportAuTransport(authKey, entities, connectionPoint, 
				clustername, distributionGroup, delay);
		main.run();
	}
}

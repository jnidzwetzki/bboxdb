/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportADSB implements Runnable {

	/**
	 * The amount of pending insert futures
	 */
	private final static int MAX_PENDING_FUTURES = 1000;
	
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
	private final String tablename;
	
	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ImportADSB.class);
	
	public ImportADSB(final String connectionPoint, final String clustername, final String tablename) {
		this.connectionPoint = connectionPoint;
		this.clustername = clustername;
		this.tablename = tablename;
		this.pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES);
	}

	@Override
	public void run() {
		
		
		try (
	    		final BBoxDB bboxdbClient = new BBoxDBCluster(connectionPoint, clustername);
				final Socket socket = new Socket("data.adsbhub.org", 5002);
	        ){
			bboxdbClient.connect();

			final TupleBuilder builder = TupleBuilderFactory.getBuilderForFormat(TupleBuilderFactory.Name.ADSB);
			
			final BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String line = null;
			
			while((line = reader.readLine()) != null) {
				if(Thread.currentThread().isInterrupted()) {
					logger.info("Thread is interrupted, returning");
					return;
				}
				
				try {
					final Tuple tuple = builder.buildTuple(line);
					if(tuple != null) {
						final EmptyResultFuture insertFuture = bboxdbClient.insertTuple(tablename, tuple);
						pendingFutures.put(insertFuture);
					}
				} catch (BBoxDBException e) {
					logger.error("Got error while inserting tuple");
				}
			}
		} catch (Exception e) {
			logger.error("Got an exception", e);
		} finally {
			waitForPendingFutures();
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
		
		if(args.length != 3) {
			System.err.println("Usage: <Class> <Connection Endpoint> <Clustername> <Table>");
			System.exit(-1);
		}
		
		final String connectionPoint = args[0];
		final String clustername = args[1];
		final String tablename = args[2];
				
		final ImportADSB main = new ImportADSB(connectionPoint, clustername, tablename);
		main.run();
	}
}

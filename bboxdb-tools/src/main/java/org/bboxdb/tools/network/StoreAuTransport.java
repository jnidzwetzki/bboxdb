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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreAuTransport implements Runnable {
	
	/**
	 * The polling delay
	 */
	private long fetchDelay;
	
	/**
	 * The Auth key
	 */
	private final String authKey;
	
	/**
	 * The output dir
	 */
	private final File output;
	
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
	private final static Logger logger = LoggerFactory.getLogger(StoreAuTransport.class);

	
	public StoreAuTransport(final String authKey, final String[] entities, final File output, final int delay) {
				this.authKey = authKey;
				this.entities = entities;
				this.output = output;
				this.fetchDelay = TimeUnit.SECONDS.toMillis(delay);
				this.threadPool = Executors.newCachedThreadPool();
	}

	@Override
	public void run() {
		
		final List<OutputStream> openStreams = new ArrayList<>();
		
		try {
			for(final String entity: entities) {
				logger.info("Starting fetch thread for {}", entity);
				
				final String urlString = AuTransportSources.API_ENDPOINT.get(entity);
				
				if(urlString == null) {
					logger.error("Unable to determine URL for: " + entity);
					System.exit(-1);
				}
				
				final File file = new File(output.getAbsolutePath() + "/" + entity);
				final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
				openStreams.add(bos);
				final Consumer<GeoJsonPolygon> consumer = (polygon) -> {
					try {
						
						if(polygon == null) {
							return;
						}
						
						bos.write(polygon.toGeoJson().getBytes());
						bos.write("\n".getBytes());
						bos.flush();
					} catch (IOException e) {
						logger.error("Got error while inserting tuple");
					}
				};
			
				final FetchRunable runable = new FetchRunable(urlString, authKey, consumer, fetchDelay, 
						entity, false);
				
				threadPool.submit(runable);
			}
			
			// Wait forever
			threadPool.shutdown();
			threadPool.awaitTermination(999999, TimeUnit.DAYS);
			
		} catch (Exception e) {
			logger.error("Got an exception", e);
		} finally {
			threadPool.shutdownNow();
			openStreams.forEach(s -> CloseableHelper.closeWithoutException(s));
		}   
	}
	
	/**
	 * Main * Main * Main * Main * Main
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		if(args.length != 4) {
			System.err.println("Usage: <Class> <AuthKey> <Entity1:Entity2:EntityN> <OutputFolder> <Delay in sec>");
			System.err.println("Entities: " + AuTransportSources.SUPPORTED_ENTITIES);
			System.exit(-1);
		}
		
		final String authKey = args[0];
		final String[] entities = args[1].split(":");
		final String outputFolder = args[2];
		final int delay = MathUtil.tryParseIntOrExit(args[3], () -> "Unable to parse delay value: " 
				+ args[3]);
		
		final File output = new File(outputFolder);
		if(! output.exists()) {
			System.err.println("Output dir does not exists: " + output);
			System.exit(-1);
		}
		
		if(! output.isDirectory()) {
			System.err.println("Output dir is not a directory: " + output);
			System.exit(-1);
		}
				
		final StoreAuTransport main = new StoreAuTransport(authKey, entities, output, delay);
		main.run();
	}
}

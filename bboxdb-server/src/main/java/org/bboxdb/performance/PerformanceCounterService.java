/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.performance;

import java.io.IOException;

import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

public class PerformanceCounterService implements BBoxDBService {
	
	/**
	 * The HTTP exposure server
	 */
	protected HTTPServer server;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(PerformanceCounterService.class);

	@Override
	public void init() throws InterruptedException, BBoxDBException {
		final BBoxDBConfiguration bboxDBConfiguration = BBoxDBConfigurationManager.getConfiguration();
		
		final int port = bboxDBConfiguration.getPerformanceCounterPort();
		
		// Service is disabled
		if(port == -1) {
			logger.info("Performance counter service is disabled");
			return;
		}
		
		logger.info("Starting performance counter service on port: {}", port);
		
   		// Expose JVM stats
		DefaultExports.initialize();
		
		try {
			server = new HTTPServer(port);
		} catch (IOException e) {
			logger.error("Got an exception during starting up performance counter HTTP sever", e);
		}
	}

	@Override
	public void shutdown() {
		if(server != null) {
			server.stop();
			server = null;
		}
	}

	@Override
	public String getServicename() {
		return "Performance counter service";
	}
}

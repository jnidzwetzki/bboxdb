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
package org.bboxdb.tools.converter.osm;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSMConverterStatistics extends ExceptionSafeRunnable {
	
	/**
	 * The amount of processed nodes
	 */
	protected double processedNodes;
	
	/**
	 * The amount of processed ways
	 */
	protected double processedWays;
	
	/**
	 * The performance timestamp
	 */
	protected long lastPerformaceTimestamp;
	
	/**
	 * The amount of processed nodes in the last call
	 */
	protected double lastProcessedElements;
	
	/**
	 * Conversion begin
	 */
	protected long beginTimestamp;
	
	/**
	 * The print thread
	 */
	protected Thread thread;
	
	/**
	 * The delay between two statistic prints
	 */
	protected final static int DELAY_IN_MS = 1000;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(OSMConverterStatistics.class);


	public void start() {
		processedNodes = 0;
		lastPerformaceTimestamp = 0;
		lastProcessedElements = 0;
		beginTimestamp = System.currentTimeMillis();
		thread = new Thread(this);
		thread.start();
	}
	
	public void stop() {
		thread.interrupt();
		
		System.out.format("Imported %.0f nodes and %.0f ways\n", 
				getProcessedNodes(), getProcessedWays());
	}
	
	@Override
	protected void runThread() throws Exception {
		
		while(! Thread.interrupted()) {
			final long now = System.currentTimeMillis();
			final double totalProcessedElements = getTotalProcessedElements();
			
			final double performanceTotal = totalProcessedElements / ((now - beginTimestamp) / (float) DELAY_IN_MS);				
			final double performanceSinceLastCall = (totalProcessedElements - lastProcessedElements) / ((now - lastPerformaceTimestamp) / (float) 1000.0);
			
			final String logMessage = String.format(
					"Processing node %.0f and way %.0f / Elements per Sec %.2f / Total elements per Sec %.2f",
					processedNodes, processedWays, performanceSinceLastCall, performanceTotal);
	
			logger.info(logMessage);
			
			lastPerformaceTimestamp = now;
			lastProcessedElements = totalProcessedElements;
			
			try {
				Thread.sleep(DELAY_IN_MS);
			} catch(InterruptedException e) {
				return;
			}
		}
	}
	
	public void incProcessedNodes() {
		processedNodes++;
	}
	
	public void incProcessedWays() {
		processedWays++;
	}
	
	public double getProcessedNodes() {
		return processedNodes;
	}
	
	public double getProcessedWays() {
		return processedWays;
	}
	
	public double getTotalProcessedElements() {
		return processedNodes + processedWays;
	}

}
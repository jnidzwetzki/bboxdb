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
package org.bboxdb.networkproxy;

import org.bboxdb.BBoxDBMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyMain implements Runnable {

	/**
	 * The contactpoint
	 */
	private final String contactpoint;
	
	/**
	 * The clustername
	 */
	private final String clustername;
	
	/**
	 * Network port
	 */
	private final int port;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBMain.class);

	public ProxyMain(final String contactpoint, final String clustername) {
		this.contactpoint = contactpoint;
		this.clustername = clustername;
		this.port = ProxyConst.PROXY_PORT;
	}

	@Override
	public void run() {
		logger.info("Starting BBoxDB proxy on port: " + port);
	}
	
	/**
	 * Main
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage: <Contactpoint> <Clustername>");
			System.exit(-1);
		}
		
		final String contactpoint = args[0];
		final String clustername = args[1];
		
		final ProxyMain main = new ProxyMain(contactpoint, clustername);
		main.run();
	}
}

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
package org.bboxdb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.distribution.SSTableFlushZookeeperAdapter;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.jmx.JMXService;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.misc.Const;
import org.bboxdb.network.server.NetworkConnectionService;
import org.bboxdb.storage.RecoveryService;
import org.bboxdb.storage.facade.StorageRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start the BBoxDB server 
 *
 */
public class BBoxDBMain {

	/**
	 * The instances to manage
	 */
	protected final List<BBoxDBService> services = new ArrayList<BBoxDBService>();

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBMain.class);

	public void init() throws Exception {
		logger.info("Init the BBoxDB");
		
		services.clear();
		
		// The zookeeper client
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		services.add(zookeeperClient);
		
		// The membership connection service
		final MembershipConnectionService membershipService = createMembershipService();
		services.add(membershipService);
		
		// The network connection handler
		final NetworkConnectionService connectionHandler = createConnectionHandler();
		services.add(connectionHandler);	
		
		// The recovery service
		final RecoveryService recoveryService = new RecoveryService(connectionHandler);
		services.add(recoveryService);
		
		// The JMX service
		final JMXService jmxService = new JMXService(this);
		services.add(jmxService);
		
		// Send flush events to zookeeper
		StorageRegistry.getInstance().registerSSTableFlushCallback(new SSTableFlushZookeeperAdapter());
	}

	/**
	 * Returns a new instance of the membership service
	 * @return
	 */
	public MembershipConnectionService createMembershipService() {
		final MembershipConnectionService membershipService = MembershipConnectionService.getInstance();
		
		// Prevent network connections to ourself
		final DistributedInstance localhost = ZookeeperClientFactory.getLocalInstanceName(BBoxDBConfigurationManager.getConfiguration());
		membershipService.addSystemToBlacklist(localhost);
		
		return membershipService;
	}

	/**
	 * Returns a new instance of the connection handler
	 * @return
	 */
	protected NetworkConnectionService createConnectionHandler() {
		return new NetworkConnectionService();
	}

	/**
	 * Start all services
	 */
	public void start() {
		logger.info("Starting up the BBoxDB - version: {}", Const.VERSION);	
		
		if (! runBaseChecks() ) {
			logger.error("Some of the base checks have failed, exiting");
			System.exit(-1);
		}
		
		// Init all services
		for(final BBoxDBService service : services) {
			try {
				logger.info("Starting service: {}", service.getServicename());
				service.init();
			} catch (Throwable e) {
				logger.error("Got exception while init service:" + service.getServicename(), e);
				stop();
				System.exit(-1);
			}
		}
		
		// Read membership
		ZookeeperClientFactory.getZookeeperClient().startMembershipObserver();
	}

	/**
	 * Run some base checks to ensure, the services could be started
	 * @return
	 */
	protected boolean runBaseChecks() {
		final List<String> dataDirs = BBoxDBConfigurationManager.getConfiguration().getStorageDirectories();
		
		for(final String dataDir : dataDirs) {
			final File dataDirHandle = new File(dataDir);
			
			// Ensure that the server main dir does exist
			if(! dataDirHandle.exists() ) {
				logger.error("Data directory does not exist: {}", dataDir);
				return false;
			}
		}
		
		return true;
	}

	/**
	 * Stop all services
	 */
	public void stop() {
		logger.info("Stopping the BBoxDB");
		
		// Stop all services
		for(final BBoxDBService service : services) {
			try {
				logger.info("Stopping service: {}", service.getServicename());
				service.shutdown();
			} catch (Throwable e) {
				logger.error("Got exception while stopping service:" + service.getServicename(), e);
			}
		}
		
		services.clear();
		logger.info("Shutdown complete");
	}

	/**
 	 * Main * Main * Main * Main * Main 
	 */
	public static void main(final String[] args) throws Exception {
		final BBoxDBMain main = new BBoxDBMain();
		main.init();
		main.start();
	}
}

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
package org.bboxdb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.io.UnsafeMemoryHelper;
import org.bboxdb.distribution.DistributedRecoveryService;
import org.bboxdb.distribution.TupleStoreFlushZookeeperAdapter;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.statistics.StatisticsUpdateService;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperInstanceRegisterer;
import org.bboxdb.jmx.JMXService;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.misc.Const;
import org.bboxdb.network.server.NetworkConnectionService;
import org.bboxdb.performance.PerformanceCounterService;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
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
	protected final List<BBoxDBService> services = new ArrayList<>();

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBMain.class);

	public void init() throws Exception {
		logger.info("Init the BBoxDB");
		
		services.clear();
		
		// The storage registry
		final TupleStoreManagerRegistry storageRegistry = new TupleStoreManagerRegistry();
		services.add(storageRegistry);
		
		// The zookeeper registerer
		final ZookeeperInstanceRegisterer zookeeperClient = new ZookeeperInstanceRegisterer();
		services.add(zookeeperClient);
		
		// The membership connection service
		final MembershipConnectionService membershipService = createMembershipService(storageRegistry);
		services.add(membershipService);
		
		// The network connection handler
		final NetworkConnectionService connectionHandler = createConnectionHandler(storageRegistry);
		services.add(connectionHandler);	
		
		// The recovery service
		final DistributedRecoveryService recoveryService = new DistributedRecoveryService(storageRegistry);
		services.add(recoveryService);
		
		// The statistics update service
		final StatisticsUpdateService statisticsService = new StatisticsUpdateService(storageRegistry);
		services.add(statisticsService);
		
		// The JMX service
		final JMXService jmxService = new JMXService(this, storageRegistry);
		services.add(jmxService);
		
		// The performance counter service
		final PerformanceCounterService performanceCounterService = new PerformanceCounterService();
		services.add(performanceCounterService);
		
		// Send flush events to zookeeper
		storageRegistry.registerSSTableFlushCallback(new TupleStoreFlushZookeeperAdapter());
	}

	/**
	 * Returns a new instance of the membership service
	 * @param storageRegistry 
	 * @return
	 */
	public MembershipConnectionService createMembershipService(
			final TupleStoreManagerRegistry storageRegistry) {
		
		final MembershipConnectionService membershipService = MembershipConnectionService.getInstance();
		
		// Prevent network connections to ourself
		final BBoxDBInstance localhost = ZookeeperClientFactory.getLocalInstanceName();
		membershipService.addSystemToBlacklist(localhost);
		
		// The storage registry for gossip
		membershipService.setTupleStoreManagerRegistry(storageRegistry);
		
		return membershipService;
	}

	/**
	 * Returns a new instance of the connection handler
	 * @return
	 */
	protected NetworkConnectionService createConnectionHandler(final TupleStoreManagerRegistry storageRegistry) {
		return new NetworkConnectionService(storageRegistry);
	}

	/**
	 * Start all services
	 */
	public void start() {
		logger.info("Starting up BBoxDB - version: {}", Const.VERSION);	
		
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
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
	}

	/**
	 * Run some base checks to ensure, the services could be started
	 * @return
	 */
	protected boolean runBaseChecks() {
		final boolean dirCheckOk = baseDirCheck();
		
		if(dirCheckOk == false) {
			return false;
		}
		
		// Check for memory mapped file unmapper
		final boolean memoryCleanerOk = UnsafeMemoryHelper.isDirectMemoryUnmapperAvailable();
		
		if(memoryCleanerOk == false) {
			logger.error("Cannot initialize memory un-mmaper. Please use a Oracle JVM");
			return false;
		}
		
		// Check for address space size
		final String datamodel = System.getProperty("sun.arch.data.model");
		if(! "64".equals(datamodel)) {
			logger.error("32 bit environment detected ({}). It is recommended to run BBoxDB on a 64 bit JVM.", datamodel);
			return false;
		}
		
		return true;
	}

	/**
	 * 
	 * @return
	 */
	protected boolean baseDirCheck() {
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

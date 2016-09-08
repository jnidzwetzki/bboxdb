package de.fernunihagen.dna.scalephant;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.MembershipConnectionService;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.scalephant.network.server.NetworkConnectionService;
import de.fernunihagen.dna.scalephant.storage.RecoveryService;

public class ScalephantMain implements Daemon {

	/**
	 * The instances to manage
	 */
	protected final List<ScalephantService> services = new ArrayList<ScalephantService>();

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantMain.class);

	@Override
	public void init(final DaemonContext ctx) throws DaemonInitException, Exception {
		logger.info("Init the scalephant");
		
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
	}

	/**
	 * Returns a new instance of the membership service
	 * @return
	 */
	public MembershipConnectionService createMembershipService() {
		final MembershipConnectionService membershipService = MembershipConnectionService.getInstance();
		
		// Prevent network connections to ourself
		final DistributedInstance localhost = ZookeeperClientFactory.getLocalInstanceName(ScalephantConfigurationManager.getConfiguration());
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

	@Override
	public void start() throws Exception {
		logger.info("Starting up the scalephant - version: " + Const.VERSION);	
		
		if (! runBaseChecks() ) {
			logger.warn("Some of the base checks have failed, exiting");
			System.exit(-1);
		}
		
		// Init all services
		for(ScalephantService service : services) {
			logger.info("Starting service: " + service.getServicename());
			service.init();
		}
		
		// Read membership
		ZookeeperClientFactory.getZookeeperClient().startMembershipObserver();
	}

	/**
	 * Run some base checks to ensure, the services could be started
	 * @return
	 */
	protected boolean runBaseChecks() {
		final String dataDir = ScalephantConfigurationManager.getConfiguration().getDataDirectory();
		final File dataDirHandle = new File(dataDir);
		
		// Ensure that the server main dir does exist
		if(! dataDirHandle.exists() ) {
			logger.error("Data directory does not exist: " + dataDir);
			return false;
		}
		
		return true;
	}

	@Override
	public void stop() throws Exception {
		logger.info("Stopping the scalephant");
		
		// Stop all services
		for(ScalephantService service : services) {
			logger.info("Stopping service: " + service.getServicename());
			service.shutdown();
		}
	}
	
	@Override
	public void destroy() {
		logger.info("Destroy the instance of the scalephant");
		services.clear();
	}
	
	//===========================================
	// Test method
	//===========================================
	public static void main(String[] args) throws DaemonInitException, Exception {
		final ScalephantMain main = new ScalephantMain();
		main.init(null);
		main.start();
	}
}

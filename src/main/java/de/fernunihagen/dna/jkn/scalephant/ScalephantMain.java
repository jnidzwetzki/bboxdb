package de.fernunihagen.dna.jkn.scalephant;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.network.server.ConnectionHandler;

public class ScalephantMain implements Daemon {

	/**
	 * The instances to manage
	 */
	protected final List<Lifecycle> services = new ArrayList<Lifecycle>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantMain.class);

	@Override
	public void init(final DaemonContext ctx) throws DaemonInitException, Exception {
		logger.info("Init the scalephant");
		
		final ScalephantConfiguration scalephantConfiguration = ScalephantConfigurationManager.getConfiguration();
		
		// The zookeeper client
		services.add(new ZookeeperClient(scalephantConfiguration.getZookeepernodes()));

		// The network conenction handler
		services.add(new ConnectionHandler());		
	}

	@Override
	public void start() throws Exception {
		logger.info("Starting up the scalephant - version: " + Const.VERSION);	
		
		// Init all services
		for(Lifecycle service : services) {
			service.init();
		}
	}

	@Override
	public void stop() throws Exception {
		logger.info("Stopping the scalephant");
		
		// Stop all services
		for(Lifecycle service : services) {
			service.shutdown();
		}
	}
	
	@Override
	public void destroy() {
		logger.info("Destroy the scalephant");
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

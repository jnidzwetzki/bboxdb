package de.fernunihagen.dna.jkn.scalephant;

import java.util.ArrayList;
import java.util.Collection;
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
	protected final List<ScalephantService> services = new ArrayList<ScalephantService>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantMain.class);

	@Override
	public void init(final DaemonContext ctx) throws DaemonInitException, Exception {
		logger.info("Init the scalephant");
		
		final ScalephantConfiguration scalephantConfiguration = ScalephantConfigurationManager.getConfiguration();
		
		// The zookeeper client
		services.add(createZookeeperClient(scalephantConfiguration));

		// The network conenction handler
		services.add(createConnectionHandler());		
	}

	/**
	 * Returns a new instance of the connection handler
	 * @return
	 */
	protected ConnectionHandler createConnectionHandler() {
		return new ConnectionHandler();
	}

	/**
	 * Returns a new instance of the zookeeper client
	 * @param scalephantConfiguration
	 * @return
	 */
	protected ZookeeperClient createZookeeperClient(
			final ScalephantConfiguration scalephantConfiguration) {
		
		final Collection<String> zookeepernodes = scalephantConfiguration.getZookeepernodes();
		final String clustername = scalephantConfiguration.getClustername();
		final String localIp = scalephantConfiguration.getLocalip();
		final int localPort = scalephantConfiguration.getNetworkListenPort();
		
		final String instanceName = localIp + ":" + Integer.toString(localPort);
		
		return new ZookeeperClient(zookeepernodes, clustername, instanceName);
	}

	@Override
	public void start() throws Exception {
		logger.info("Starting up the scalephant - version: " + Const.VERSION);	
		
		// Init all services
		for(ScalephantService service : services) {
			logger.info("Starting service: " + service.getServicename());
			service.init();
		}
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

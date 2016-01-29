package de.fernunihagen.dna.jkn.scalephant;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.ConnectionHandler;

public class Main implements Daemon {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	
	/**
	 * The network connection handler
	 */
	protected ConnectionHandler connectionHandler = null;

	@Override
	public void init(final DaemonContext ctx) throws DaemonInitException, Exception {
		logger.info("Init the scalephant");
		connectionHandler = new ConnectionHandler();
	}

	@Override
	public void start() throws Exception {
		logger.info("Starting up the scalephant - version: " + Const.VERSION);		
		connectionHandler.init();
	}

	@Override
	public void stop() throws Exception {
		logger.info("Stopping the scalephant");
		connectionHandler.shutdown();
	}
	
	@Override
	public void destroy() {
		logger.info("Destroy the scalephant");
		connectionHandler = null;
	}
	
	public static void main(String[] args) throws DaemonInitException, Exception {
		final Main main = new Main();
		main.init(null);
		main.start();
	}
}

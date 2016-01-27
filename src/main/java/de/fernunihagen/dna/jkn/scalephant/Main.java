package de.fernunihagen.dna.jkn.scalephant;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main implements Daemon {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Main.class);

	@Override
	public void destroy() {
		logger.info("Destroy the scalephant");
	}

	@Override
	public void init(final DaemonContext ctx) throws DaemonInitException, Exception {
		logger.info("Init the scalephant");
	}

	@Override
	public void start() throws Exception {
		logger.info("Starting up the scalephant - version: " + Const.VERSION);		
	}

	@Override
	public void stop() throws Exception {
		logger.info("Stopping the scalephant");
	}

}

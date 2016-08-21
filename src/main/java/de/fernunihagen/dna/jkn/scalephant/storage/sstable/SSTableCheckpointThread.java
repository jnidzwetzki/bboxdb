package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.util.Stoppable;

public class SSTableCheckpointThread implements Runnable, Stoppable {

	/**
	 * The sstable manager
	 */
	protected final SSTableManager ssTableManager;
	
	/**
	 * The maximal number of seconds for data to stay in memory
	 */
	protected final int maxUncheckpointedSeconds;
	
	/**
	 * The run variable
	 */
	protected volatile boolean run;
	
	/**
	 * The delay
	 */
	protected final long DELAY = TimeUnit.SECONDS.toMillis(60);
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCheckpointThread.class);
	
	public SSTableCheckpointThread(final int maxUncheckpointedSeconds, final SSTableManager ssTableManager) {
		this.maxUncheckpointedSeconds = maxUncheckpointedSeconds;
		this.ssTableManager = ssTableManager;
		this.run = true;
	}

	@Override
	public void run() {
		while(run) {
			
			logger.info("Executing checkpoint thread for: " + ssTableManager.getSSTableName());
			
			try {
				Thread.sleep(DELAY);
			} catch (InterruptedException e) {
				logger.info("Got interrupted exception, stopping thread");
				return;
			}
		}
	}

	@Override
	public void stop() {
		run = false;
	}

}

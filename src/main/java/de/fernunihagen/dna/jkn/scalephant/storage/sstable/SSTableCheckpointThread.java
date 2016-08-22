package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.Memtable;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.util.Stoppable;

public class SSTableCheckpointThread implements Runnable, Stoppable {

	/**
	 * The sstable manager
	 */
	protected final SSTableManager ssTableManager;
	
	/**
	 * The maximal number of seconds for data to stay in memory
	 */
	protected final long maxUncheckpointedMiliseconds;
	
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
		this.maxUncheckpointedMiliseconds = TimeUnit.SECONDS.toMillis(maxUncheckpointedSeconds);
		this.ssTableManager = ssTableManager;
		this.run = true;
	}

	@Override
	public void run() {
		
		while(run) {
			logger.info("Executing checkpoint thread for: " + ssTableManager.getSSTableName());
			
			createCheckpoint();
		
			try {
				Thread.sleep(DELAY);
			} catch (InterruptedException e) {
				logger.info("Got interrupted exception, stopping thread");
				return;
			}
		}
	}
	
	/**
	 * Decide if a new checkpoint is needed
	 * @return
	 */
	protected boolean isCheckpointNeeded() {
		
		final List<Memtable> memtablesToCheck = new ArrayList<Memtable>();
		memtablesToCheck.add(ssTableManager.getMemtable());
		memtablesToCheck.addAll(ssTableManager.getUnflushedMemtables());
	
		for(final Memtable memtable : memtablesToCheck) {
			long memtableCreated = memtable.getCreatedTimestamp();
	
			// Active memtable is to old
			if(memtableCreated + maxUncheckpointedMiliseconds < System.currentTimeMillis()) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Create a new checkpoint, this means flush all old memtables to disk
	 */
	protected void createCheckpoint() {
		try {

			// Is a new checkpoint needed?
			if(! isCheckpointNeeded()) {
				return;
			}
			
			final Memtable activeMemtable = ssTableManager.getMemtable();
			logger.info("Creating a checkpoint for: " + ssTableManager.getSSTableName());
			ssTableManager.flushMemtable();
			
			final List<Memtable> unflushedMemtables = ssTableManager.getUnflushedMemtables();
			
			// Wait until the active memtable is flushed to disk
			synchronized (unflushedMemtables) {
				while(unflushedMemtables.contains(activeMemtable)) {
					unflushedMemtables.wait();
				}
			}
			
			final long createdTimestamp = activeMemtable.getCreatedTimestamp();
			updateCheckpointDate(createdTimestamp);
			
			logger.info("Create checkpoint DONE for: " + ssTableManager.getSSTableName() + " timestamp " + createdTimestamp);
		} catch (StorageManagerException e) {
			logger.warn("Got an exception while creating checkpoint", e);
		} catch (InterruptedException e) {
			logger.warn("Got an exception while creating checkpoint", e);
		}
	}

	/**
	 * Update the checkpoint date (e.g. propergate checkpoint to zookeeper)
	 * @param createdTimestamp
	 */
	protected void updateCheckpointDate(final long createdTimestamp) {
		
	}

	@Override
	public void stop() {
		run = false;
	}

}

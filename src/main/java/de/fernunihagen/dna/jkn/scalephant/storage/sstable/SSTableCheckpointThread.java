package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionGroupCache;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegionHelper;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.zookeeper.ZookeeperClientFactory;
import de.fernunihagen.dna.jkn.scalephant.distribution.zookeeper.ZookeeperException;
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
	 * The name of the local instance
	 */
	protected DistributedInstance localInstance;
	
	/**
	 * The distribution region of the sstable
	 */
	protected DistributionRegion distributionRegion = null;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCheckpointThread.class);

	public SSTableCheckpointThread(final int maxUncheckpointedSeconds, final SSTableManager ssTableManager) {
		this.maxUncheckpointedMiliseconds = TimeUnit.SECONDS.toMillis(maxUncheckpointedSeconds);
		this.ssTableManager = ssTableManager;
		this.run = true;
		
		// Local instance
		final ScalephantConfiguration scalephantConfiguration = ScalephantConfigurationManager.getConfiguration();
		this.localInstance = ZookeeperClientFactory.getLocalInstanceName(scalephantConfiguration);
	
		// Distribution region
		/**
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final DistributionRegion distributionGroupRoot = DistributionGroupCache.getGroupForTableName(ssTableManager.getSSTableName().getFullname(), zookeeperClient);
			distributionRegion = DistributionRegionHelper.getDistributionRegionForNamePrefix(distributionGroupRoot, ssTableManager.getSSTableName().getNameprefix());
		} catch (ZookeeperException e) {
			logger.warn("Unable to find distribution region: " , e);
		}*/
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
		} catch (ZookeeperException e) {
			logger.error("Got an exception while updating checkpoint", e);
		}
	}

	/**
	 * Update the checkpoint date (e.g. propagate checkpoint to zookeeper)
	 * @param createdTimestamp
	 * @throws ZookeeperException 
	 */
	protected void updateCheckpointDate(final long checkpointTimestamp) throws ZookeeperException {
		if(distributionRegion != null) {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			zookeeperClient.setCheckpointForDistributionRegion(distributionRegion, localInstance, checkpointTimestamp);
		}
	}

	@Override
	public void stop() {
		run = false;
	}

}

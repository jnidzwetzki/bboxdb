package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionWithZookeeperIntegration extends DistributionRegion implements Watcher {

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionWithZookeeperIntegration.class);
	
	public DistributionRegionWithZookeeperIntegration(final DistributionGroupName name, final int level, final TotalLevel totalLevel, final ZookeeperClient zookeeperClient) {
		super(name, level, totalLevel);
		this.zookeeperClient = zookeeperClient;
	}
	
	/**
	 * The node complete event
	 */
	public void onNodeComplete() {
		final String zookeeperPath = zookeeperClient.getZookeeperPathForDistributionRegion(this);
		try {
			logger.info("Register watch for: " + zookeeperPath);
			zookeeperClient.getChildren(zookeeperPath, this);
		} catch (ZookeeperException e) {
			logger.info("Unable to register watch for: " + zookeeperPath, e);
		}
	}

	/**
	 * Process structure updates (e.g. changes in the distribution group)
	 */
	@Override
	public void process(final WatchedEvent event) {
		final String zookeeperPath = zookeeperClient.getZookeeperPathForDistributionRegion(this);
		logger.info("Node: " + zookeeperPath + " got event: " + event);
		
		if(! isLeafRegion()) {
			logger.debug("Ignore update events on ! leafRegions");
			return;
		}
		
		try {
			// Does the split position exists?
			logger.info("Read for: " + zookeeperPath);
			final List<String> childs = zookeeperClient.getChildren(zookeeperPath, this);
			boolean splitExists = false;
			
			for(final String child : childs) {
				if(child.endsWith(ZookeeperClient.NAME_SPLIT)) {
					splitExists = true;
					break;
				}
			}
			
			// Wait for a new event of the creation for the split position
			if(! splitExists) {
				return;
			}
			
			// Update data structure with data from zookeeper
			zookeeperClient.readDistributionGroupRecursive(zookeeperPath, this);

		} catch (ZookeeperException e) {
			logger.error("Unable read data from zookeeper: ", e);
		}
	}
	
	/**
	 * Create a new instance of this type
	 */
	@Override
	protected DistributionRegion createNewInstance() {
		return new DistributionRegionWithZookeeperIntegration(distributionGroupName, level + 1, totalLevel, zookeeperClient);
	}
	
	/**
	 * Propergate the split position to zookeeper
	 */
	@Override
	protected void afterSplitHook(final boolean sendNotify) {
		// Update zookeeper (if this is a call from a user)
		try {
			if(sendNotify == true) {
				logger.debug("Propergate split to zookeeper");
				updateZookeeperSplit();
				logger.debug("Propergate split to zookeeper done");
			}
		} catch (ZookeeperException e) {
			logger.error("Unable to update split in zookeeper: ", e);
		}
	}
	
	/**
	 * Update zookeeper after splitting an region
	 * @param distributionRegion
	 * @throws ZookeeperException 
	 */
	protected void updateZookeeperSplit() throws ZookeeperException {

		final String zookeeperPath = zookeeperClient.getZookeeperPathForDistributionRegion(this);
		
		// Left child
		final String leftPath = zookeeperPath + "/" + ZookeeperClient.NODE_LEFT;
		logger.debug("Create: " + leftPath);
		zookeeperClient.createPersistentNode(leftPath, "".getBytes());
		
		final int leftNamePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		zookeeperClient.createPersistentNode(leftPath + "/" + ZookeeperClient.NAME_NAMEPREFIX, 
				Integer.toString(leftNamePrefix).getBytes());
		
		// Right child
		final String rightPath = zookeeperPath + "/" + ZookeeperClient.NODE_RIGHT;
		logger.debug("Create: " + rightPath);
		zookeeperClient.createPersistentNode(rightPath, "".getBytes());
		
		final int rightNamePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		zookeeperClient.createPersistentNode(rightPath + "/" + ZookeeperClient.NAME_NAMEPREFIX, 
				Integer.toString(rightNamePrefix).getBytes());
		
		// Last step: write split position
		final String splitPosString = Float.toString(getSplit());
		zookeeperClient.createPersistentNode(zookeeperPath + "/" + ZookeeperClient.NAME_SPLIT, 
				splitPosString.getBytes());
	}

}

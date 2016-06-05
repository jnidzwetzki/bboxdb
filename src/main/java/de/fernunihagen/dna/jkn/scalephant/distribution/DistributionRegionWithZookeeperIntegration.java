package de.fernunihagen.dna.jkn.scalephant.distribution;

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
	 * Process structure updates (e.g. changes in the distribution group)
	 */
	@Override
	public void process(final WatchedEvent event) {
		
	}
	
	/**
	 * Create a new instance of this type
	 */
	@Override
	protected DistributionRegion createNewInstance() {
		return new DistributionRegionWithZookeeperIntegration(distributionGroupName, level, totalLevel, zookeeperClient);
	}
	
	/**
	 * Set the split position and propergate this to zookeeper (if this not a structure build call)
	 */
	@Override
	public void setSplit(final float split, final boolean sendNotify) {
		// Update data structure
		super.setSplit(split, sendNotify);
		
		// Update zookeeper (if this is a call from a user)
		try {
			if(sendNotify == true) {
				updateZookeeperSplit();
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

		final String zookeeperPath = getZookeeperPathForDistributionRegion(this);
		
		// Write split position
		final String splitPosString = Float.toString(getSplit());
		zookeeperClient.createPersistentNode(zookeeperPath + "/" + ZookeeperClient.NAME_SPLIT, 
				splitPosString.getBytes());
		
		// Left child
		final String leftPath = zookeeperPath + "/" + ZookeeperClient.NODE_LEFT;
		
		zookeeperClient.createPersistentNode(leftPath, "".getBytes());
		
		final int leftNamePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		
		zookeeperClient.createPersistentNode(leftPath + "/" + ZookeeperClient.NAME_NAMEPREFIX, 
				Integer.toString(leftNamePrefix).getBytes());
		
		// Right child
		final String rightPath = zookeeperPath + "/" + ZookeeperClient.NODE_RIGHT;
		zookeeperClient.createPersistentNode(rightPath, "".getBytes());
		
		final int rightNamePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		
		zookeeperClient.createPersistentNode(rightPath + "/" + ZookeeperClient.NAME_NAMEPREFIX, 
				Integer.toString(rightNamePrefix).getBytes());

	}

	/**
	 * Get the zookeeper path for a distribution region
	 * @param distributionRegion
	 * @return
	 */
	protected String getZookeeperPathForDistributionRegion(final DistributionRegion distributionRegion) {
		final String name = distributionRegion.getName();
		final StringBuilder sb = new StringBuilder();
		
		DistributionRegion tmpRegion = distributionRegion;
		while(tmpRegion.getParent() != null) {
			if(tmpRegion.isLeftChild()) {
				sb.insert(0, "/" + ZookeeperClient.NODE_LEFT);
			} else {
				sb.insert(0, "/" + ZookeeperClient.NODE_RIGHT);
			}
			
			tmpRegion = tmpRegion.getParent();
		}
		
		sb.insert(0, zookeeperClient.getDistributionGroupPath(name));
		return sb.toString();
	}

}

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
	 * The full path to this node
	 */
	protected String zookeeperPath;
	
	/**
	 * The path to the systems
	 */
	protected String zookeeperSystemsPath;
	
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
		try {
			// Update zookeeper path
			zookeeperPath = zookeeperClient.getZookeeperPathForDistributionRegion(this);
			zookeeperSystemsPath = zookeeperPath + "/" + ZookeeperClient.NAME_SYSTEMS;
			
			logger.info("Register watch for: " + zookeeperPath);
			zookeeperClient.getChildren(zookeeperPath, this);
			
			logger.info("Register watch for: " + zookeeperSystemsPath);
			zookeeperClient.getChildren(zookeeperSystemsPath, this);
		} catch (ZookeeperException e) {
			logger.info("Unable to register watch for: " + zookeeperPath, e);
		} finally {
			// The node is ready and can be used
			ready = true;
		}
	}

	/**
	 * Process structure updates (e.g. changes in the distribution group)
	 */
	@Override
	public void process(final WatchedEvent event) {
		logger.info("Node: " + zookeeperPath + " got event: " + event);

		if(event.getPath().endsWith(zookeeperPath)) {
			handleNodeUpdateEvent();
		} else if(event.getPath().endsWith(zookeeperSystemsPath)) {
			handleSystemNodeUpdateEvent();
		}
	}

	/**
	 * Handle a zookeeper update for the system node
	 */
	protected void handleSystemNodeUpdateEvent() {
		try {
			logger.debug("Got an system node event for: " + zookeeperSystemsPath);
			setSystems(zookeeperClient.getSystemsForDistributionRegion(this));
		} catch (ZookeeperException e) {
			logger.error("Unable read data from zookeeper: ", e);
		}
	}

	/**
	 * Handle a zookeeper update for the node
	 * @throws ZookeeperException
	 */
	protected void handleNodeUpdateEvent() {
		try {
			logger.debug("Got an node event for: " + zookeeperPath);
			
			if(! isLeafRegion()) {
				logger.debug("Ignore update events on ! leafRegions");
				return;
			}
			
			// Does the split position exists?
			logger.info("Read for: " + zookeeperPath);
			final List<String> childs = zookeeperClient.getChildren(zookeeperPath, this);
			
			// Was the node deleted?
			if(childs == null) {
				return;
			}
			
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
			
			// Ignore split event, when we are already splitted
			// E.g. setSplit is called localy and written into zookeeper
			// the zookeeper callback will call setSplit again
			if(leftChild != null || rightChild != null) {
				logger.debug("Ignore zookeeper split, because we are already splited");
			} else {
				zookeeperClient.readDistributionGroupRecursive(zookeeperPath, this);
			}
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
				logger.debug("Propergate split to zookeeper: " + zookeeperPath);
				updateZookeeperSplit();
				logger.debug("Propergate split to zookeeper done: " + zookeeperPath);
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
		
		logger.debug("Write split into zookeeper");
		final String zookeeperPath = zookeeperClient.getZookeeperPathForDistributionRegion(this);
		
		// Left child
		final String leftPath = zookeeperPath + "/" + ZookeeperClient.NODE_LEFT;
		logger.debug("Create: " + leftPath);
		zookeeperClient.createPersistentNode(leftPath, "".getBytes());
		
		final int leftNamePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		zookeeperClient.createPersistentNode(leftPath + "/" + ZookeeperClient.NAME_NAMEPREFIX, 
				Integer.toString(leftNamePrefix).getBytes());
		zookeeperClient.createPersistentNode(leftPath + "/" + ZookeeperClient.NAME_SYSTEMS, 
				"".getBytes());
		
		// Right child
		final String rightPath = zookeeperPath + "/" + ZookeeperClient.NODE_RIGHT;
		logger.debug("Create: " + rightPath);
		zookeeperClient.createPersistentNode(rightPath, "".getBytes());
		
		final int rightNamePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		zookeeperClient.createPersistentNode(rightPath + "/" + ZookeeperClient.NAME_NAMEPREFIX, 
				Integer.toString(rightNamePrefix).getBytes());
		zookeeperClient.createPersistentNode(rightPath + "/" + ZookeeperClient.NAME_SYSTEMS, 
				"".getBytes());
		
		// Last step: write split position
		final String splitPosString = Float.toString(getSplit());
		zookeeperClient.createPersistentNode(zookeeperPath + "/" + ZookeeperClient.NAME_SPLIT, 
				splitPosString.getBytes());
	}
}

package de.fernunihagen.dna.scalephant.distribution;

import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.nameprefix.NameprefixInstanceManager;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;

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
			zookeeperSystemsPath = zookeeperPath + "/" + ZookeeperClient.NodeNames.NAME_SYSTEMS;
			
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
		
		// Ignore events like connected and disconnected
		if(event == null || event.getPath() == null) {
			return;
		}
		
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
			final Collection<DistributedInstance> systemsForDistributionRegion = zookeeperClient.getSystemsForDistributionRegion(this);

			if(systemsForDistributionRegion != null) {
				setSystems(systemsForDistributionRegion);
			}
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
				if(child.endsWith(ZookeeperClient.NodeNames.NAME_SPLIT)) {
					splitExists = true;
					break;
				}
			}
			
			// Wait for a new event of the creation for the split position
			if(! splitExists) {
				return;
			}
			
			// Ignore split event, when we are already split
			// E.g. setSplit is called locally and written into zookeeper
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
	
	@Override
	public void setSystems(final Collection<DistributedInstance> systems) {
		super.setSystems(systems);
		
		// Add the mapping to the nameprefix mapper
		if(systems.contains(zookeeperClient.getInstancename())) {
			logger.info("Add local mapping for: " + distributionGroupName + " nameprefix " + nameprefix);
			NameprefixInstanceManager.getInstance(distributionGroupName).addMapping(nameprefix, converingBox);
		}
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
		final String leftPath = zookeeperPath + "/" + ZookeeperClient.NodeNames.NAME_LEFT;
		logger.debug("Create: " + leftPath);
		createNewChild(leftPath, leftChild);

		// Right child
		final String rightPath = zookeeperPath + "/" + ZookeeperClient.NodeNames.NAME_RIGHT;
		logger.debug("Create: " + rightPath);
		createNewChild(rightPath, rightChild);
		
		// Write split position and update state
		final String splitPosString = Float.toString(getSplit());
		zookeeperClient.createPersistentNode(zookeeperPath + "/" + ZookeeperClient.NodeNames.NAME_SPLIT, 
				splitPosString.getBytes());
		zookeeperClient.setStateForDistributionGroup(zookeeperPath, STATE_SPLITTING);
		zookeeperClient.setStateForDistributionGroup(leftPath, STATE_ACTIVE);
		zookeeperClient.setStateForDistributionGroup(rightPath, STATE_ACTIVE);
	}

	/**
	 * Create a new child
	 * @param path
	 * @throws ZookeeperException
	 */
	protected void createNewChild(final String path, final DistributionRegion child) throws ZookeeperException {
		zookeeperClient.createPersistentNode(path, "".getBytes());
		
		final int namePrefix = zookeeperClient.getNextTableIdForDistributionGroup(getName());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperClient.NodeNames.NAME_NAMEPREFIX, 
				Integer.toString(namePrefix).getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperClient.NodeNames.NAME_SYSTEMS, 
				"".getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperClient.NodeNames.NAME_STATE, 
				STATE_CREATING.getBytes());
		
		child.setNameprefix(namePrefix);
		
		zookeeperClient.setStateForDistributionGroup(path, STATE_ACTIVE);
	}
}

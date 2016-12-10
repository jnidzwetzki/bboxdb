/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package de.fernunihagen.dna.scalephant.distribution;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.mode.DistributionGroupZookeeperAdapter;
import de.fernunihagen.dna.scalephant.distribution.mode.NodeState;
import de.fernunihagen.dna.scalephant.distribution.nameprefix.NameprefixInstanceManager;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperClient;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperException;
import de.fernunihagen.dna.scalephant.distribution.zookeeper.ZookeeperNodeNames;

public class DistributionRegionWithZookeeperIntegration extends DistributionRegion implements Watcher {

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
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

	public DistributionRegionWithZookeeperIntegration(final DistributionGroupName name, 
			final DistributionRegion parent, final ZookeeperClient zookeeperClient) {
		
		super(name, parent);
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
	}
	
	/**
	 * The node complete event
	 */
	public void onNodeComplete() {
		try {
			// Update zookeeper path
			zookeeperPath = distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(this);
			zookeeperSystemsPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_SYSTEMS;
			
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
			final Collection<DistributedInstance> systemsForDistributionRegion = distributionGroupZookeeperAdapter.getSystemsForDistributionRegion(this);

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
				if(child.endsWith(ZookeeperNodeNames.NAME_SPLIT)) {
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
				distributionGroupZookeeperAdapter.readDistributionGroupRecursive(zookeeperPath, this);
			}
		} catch (ZookeeperException e) {
			logger.error("Unable read data from zookeeper: ", e);
		}
	}
	
	/**
	 * Create a new instance of this type
	 */
	@Override
	protected DistributionRegion createNewInstance(final DistributionRegion parent) {
		return new DistributionRegionWithZookeeperIntegration(distributionGroupName, parent, zookeeperClient);
	}
	
	@Override
	public void setSystems(final Collection<DistributedInstance> systems) {
		super.setSystems(systems);
		
		if(zookeeperClient.getInstancename() == null) {
			// Local instance name is not set, so no local mapping is possible
			return;
		}
		
		final InetSocketAddress localInetSocketAddress = zookeeperClient.getInstancename().getInetSocketAddress();
		
		// Add the mapping to the nameprefix mapper
		for(final DistributedInstance instance : systems) {
			if(instance.getInetSocketAddress().equals(localInetSocketAddress)) {
				logger.info("Add local mapping for: " + distributionGroupName + " nameprefix " + nameprefix);
				NameprefixInstanceManager.getInstance(distributionGroupName).addMapping(nameprefix, converingBox);
				break;
			}
		}
	}
	
	/**
	 * Propagate the split position to zookeeper
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
		final String zookeeperPath = distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(this);
		
		// Left child
		final String leftPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_LEFT;
		logger.debug("Create: " + leftPath);
		createNewChild(leftPath, leftChild);

		// Right child
		final String rightPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_RIGHT;
		logger.debug("Create: " + rightPath);
		createNewChild(rightPath, rightChild);
		
		// Write split position and update state
		final String splitPosString = Float.toString(getSplit());
		zookeeperClient.createPersistentNode(zookeeperPath + "/" + ZookeeperNodeNames.NAME_SPLIT, 
				splitPosString.getBytes());
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPath, NodeState.SPLITTING);
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(leftPath, NodeState.ACTIVE);
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(rightPath, NodeState.ACTIVE);
	}

	/**
	 * Create a new child
	 * @param path
	 * @throws ZookeeperException
	 */
	protected void createNewChild(final String path, final DistributionRegion child) throws ZookeeperException {
		zookeeperClient.createPersistentNode(path, "".getBytes());
		
		final int namePrefix = distributionGroupZookeeperAdapter.getNextTableIdForDistributionGroup(getName());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
				Integer.toString(namePrefix).getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
				"".getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_STATE, 
				NodeState.CREATING.getStringValue().getBytes());
		
		child.setNameprefix(namePrefix);
		
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(path, NodeState.ACTIVE);
	}
}

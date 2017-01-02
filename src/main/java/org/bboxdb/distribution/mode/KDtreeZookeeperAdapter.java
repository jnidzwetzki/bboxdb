/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.distribution.mode;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.DistributedInstanceManager;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.placement.ResourcePlacementStrategy;
import org.bboxdb.distribution.placement.ResourcePlacementStrategyFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.storage.entity.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KDtreeZookeeperAdapter implements Watcher {
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The name of the distribution group
	 */
	protected final String distributionGroup;
	
	/**
	 * The root node of the K-D-Tree
	 */
	protected DistributionRegion rootNode;
	
	/**
	 * The version of the distribution group
	 */
	protected String version;
	
	/**
	 * The mutex for sync operations
	 */
	protected final Object MUTEX = new Object();
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KDtreeZookeeperAdapter.class);

	public KDtreeZookeeperAdapter(final ZookeeperClient zookeeperClient,
			final DistributionGroupZookeeperAdapter distributionGroupAdapter,
			final String distributionGroup) throws ZookeeperException {
		
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupZookeeperAdapter = distributionGroupAdapter;
		this.distributionGroup = distributionGroup;
		
		readAndHandleVersion();
	}
	

	/**
	 * Reread and handle the dgroup version
	 * @param distributionGroupName
	 * @throws ZookeeperException
	 */
	protected void readAndHandleVersion() throws ZookeeperException {
		
		try {
			final String zookeeperVersion 
				= distributionGroupZookeeperAdapter.getVersionForDistributionGroup(
					distributionGroup, this);
			
			if(version == null || ! zookeeperVersion.equals(version)) {
				// First read after start
				version = zookeeperVersion;
				handleNewRootElement();
			} 
			
		} catch (ZookeeperNotFoundException e) {
			logger.info("Version for {} not found, deleting in memory version", distributionGroup);
			handleRootElementDeleted();
		}
		
		registerDistributionGroupChangeListener();
	}
	
	/**
	 * Register a listener for distribution group changes
	 */
	protected void registerDistributionGroupChangeListener() {
		try {
			final List<DistributionGroupName> distributionGroups = distributionGroupZookeeperAdapter.getDistributionGroups(this);
			
			// Is group already in creation?
			final DistributionGroupName distributionGroupName = new DistributionGroupName(distributionGroup);
			if(rootNode == null && distributionGroups.contains(distributionGroupName)) {
				waitForGroupToAppear();
			}
			
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			logger.warn("Got exception while registering event lister for distribution group changes");
		}
	}

	/**
	 * Wait for the state node of the new distribution group
	 * @throws ZookeeperException
	 */
	protected void waitForGroupToAppear() throws ZookeeperException{
		final String dgroupPath = distributionGroupZookeeperAdapter.getDistributionGroupPath(distributionGroup);
		
		// Wait for state node to appear
		for(int retry = 0; retry < 10; retry++) {
			try {
				distributionGroupZookeeperAdapter.getStateForDistributionRegion(dgroupPath, null);
				handleNewRootElement();
				break;
			} catch (ZookeeperNotFoundException e) {
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					return;
				}
				
			}
		}
		
		if(rootNode == null) {
			logger.error("DGroup was created but state field does not appear...");
		}
	}
	
	/**
	 * Reread and handle the dgroup version
	 */
	protected void readAndHandleVersionNE() {
		try {
			readAndHandleVersion();
		} catch (ZookeeperException e) {
			logger.warn("Got zookeeper exception", e);
		}
	}
	
	/**
	 * Create and reread the distribution group
	 * @throws ZookeeperException
	 */
	protected void handleNewRootElement() throws ZookeeperException {

		// Delete old mappings
		handleRootElementDeleted();
		
		logger.info("Create new root element for {}", distributionGroup);
		rootNode = DistributionRegion.createRootElement(distributionGroup);
			
		final String path = distributionGroupZookeeperAdapter.getDistributionGroupPath(distributionGroup);
		readDistributionGroupRecursive(path, rootNode);
	}
	
	/**
	 * The root element is deleted
	 */
	public void handleRootElementDeleted() {
		logger.info("Root element for {} is deleted", distributionGroup);
		RegionIdMapperInstanceManager.getInstance(new DistributionGroupName(distributionGroup)).clear();
		rootNode = null;
	}
	
	/**
	 * Get the root node
	 * @return
	 */
	public DistributionRegion getRootNode() {
		return rootNode;
	}
	
	/**
	 * Wait until the root node is created
	 * @return
	 */
	public DistributionRegion getAndWaitForRootNode() {
		synchronized (MUTEX) {
			while(rootNode == null) {
				try {
					MUTEX.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
		
		return rootNode;
	}

	@Override
	public void process(final WatchedEvent event) {
		
		// Ignore events like connected and disconnected
		if(event == null || event.getPath() == null) {
			return;
		}
		
		final String path = event.getPath();

		if(path.equals(distributionGroupZookeeperAdapter.getClusterPath())) {
			// Amount of distribution groups have changed
			readAndHandleVersionNE();
		} else if(path.endsWith(ZookeeperNodeNames.NAME_SYSTEMS)) {
			// Some systems were added or deleted
			handleSystemNodeUpdateEvent(event);
		} else if(path.endsWith(ZookeeperNodeNames.NAME_STATE)) {
			// The state of one has changed
			handleNodeUpdateEvent(event);
		} else {
			logger.info("Ingoring event for path: {}" , path);
		}
	}
	
	/**
	 * Handle node updates
	 * @param event
	 */
	protected void handleNodeUpdateEvent(final WatchedEvent event) {
	
		if(rootNode == null) {
			logger.debug("Ignore systems update event, because root not node is null: {}", distributionGroup);
			return;
		}
		
		// Remove state node from path
		final String path = event.getPath().replace("/" + ZookeeperNodeNames.NAME_STATE, "");
		
		final DistributionRegion nodeToUpdate = distributionGroupZookeeperAdapter.getNodeForPath(rootNode, path);
		
		try {
			final String distributionGroupName = rootNode.getDistributionGroupName().getFullname();
			
			if(! distributionGroupZookeeperAdapter.isDistributionGroupRegistered(distributionGroupName)) {
				logger.info("Distribution group was unregistered, ignore event");
				return;
			}
			
			readDistributionGroupRecursive(path, nodeToUpdate);
		} catch (ZookeeperException e) {
			logger.warn("Got exception while updating node for: " + path, e);
		}
	}

	/**
	 * Handle system update events
	 * @param event
	 */
	protected void handleSystemNodeUpdateEvent(final WatchedEvent event) {
		
		if(rootNode == null) {
			logger.debug("Ignore systems update event, because root not node is null: {}", distributionGroup);
			return;
		}
		
		final String path = event.getPath().replace("/" + ZookeeperNodeNames.NAME_SYSTEMS, "");
		
		final DistributionRegion nodeToUpdate = distributionGroupZookeeperAdapter.getNodeForPath(rootNode, path);
		
		try {
			updateSystemsForRegion(nodeToUpdate);
		} catch (ZookeeperException e) {
			logger.warn("Got exception while updating systems for: " + path, e);
		}
	}

	/**
	 * Split the node at the given position
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException 
	 */
	public void splitNode(final DistributionRegion regionToSplit, final float splitPosition) throws ZookeeperException, ResourceAllocationException {
		logger.debug("Write split at pos {} into zookeeper", splitPosition);
		final String zookeeperPath = distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(regionToSplit);
		
		final String leftPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_LEFT;
		createNewChild(leftPath);
		
		final String rightPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_RIGHT;
		createNewChild(rightPath);
		
		// Write split position
		distributionGroupZookeeperAdapter.setSplitPositionForPath(zookeeperPath, splitPosition);
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPath, DistributionRegionState.SPLITTING);
		
		waitForChildCreateZookeeperCallback(regionToSplit);

		// Allocate systems 
		allocateSystemsToNewRegion(regionToSplit.getLeftChild());
		allocateSystemsToNewRegion(regionToSplit.getRightChild());
		
		// update state
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(leftPath, DistributionRegionState.ACTIVE);
		distributionGroupZookeeperAdapter.setStateForDistributionGroup(rightPath, DistributionRegionState.ACTIVE);	
	
		waitForSplitZookeeperCallback(regionToSplit);
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	public void waitForChildCreateZookeeperCallback(final DistributionRegion regionToSplit) {
		
		// Wait for zookeeper callback
		synchronized (MUTEX) {
			while(regionToSplit.isLeafRegion()) {
				logger.debug("Wait for zookeeper callback for split for: {}", regionToSplit);
				try {
					MUTEX.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.warn("Unable to wait for split for); {}", regionToSplit);
				}
			}
		}
	}
	
	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	public void waitForSplitZookeeperCallback(final DistributionRegion regionToSplit) {
		
		// Wait for zookeeper callback
		synchronized (MUTEX) {
			while(! isSplitForNodeComplete(regionToSplit)) {
				logger.debug("Wait for zookeeper callback for split for: {}", regionToSplit);
				try {
					MUTEX.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.warn("Unable to wait for split for); {}", regionToSplit);
				}
			}
		}
	}
	
	/**
	 * Allocate the required amount of systems to the given region
	 * 
	 * @param region
	 * @param zookeeperClient
	 * @throws ZookeeperException
	 * @throws ResourceAllocationException
	 */
	public void allocateSystemsToNewRegion(final DistributionRegion region) throws ZookeeperException, ResourceAllocationException {
		
		final String distributionGroupName = region.getDistributionGroupName().getFullname();
		final short replicationFactor = distributionGroupZookeeperAdapter.getReplicationFactorForDistributionGroup(distributionGroupName);
		
		final DistributedInstanceManager distributedInstanceManager = DistributedInstanceManager.getInstance();
		final List<DistributedInstance> availableSystems = distributedInstanceManager.getInstances();
		
		final ResourcePlacementStrategy resourcePlacementStrategy = ResourcePlacementStrategyFactory.getInstance();

		// The blacklist, to prevent duplicate allocations
		final Set<DistributedInstance> allocationSystems = new HashSet<DistributedInstance>();
		
		for(short i = 0; i < replicationFactor; i++) {
			final DistributedInstance instance = resourcePlacementStrategy.getInstancesForNewRessource(availableSystems, allocationSystems);
			allocationSystems.add(instance);
		}
		
		logger.info("Allocating region {} to {}", region.getIdentifier(), allocationSystems);
		
		// Resource allocation successfully, write data to zookeeper
		for(final DistributedInstance instance : allocationSystems) {
			distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, instance);
		}
	}
	
	/**
	 * Is the split for the given node complete?
	 * @param region
	 * @return
	 */
	protected boolean isSplitForNodeComplete(final DistributionRegion region) {
		
		if(region.getSplit() == Float.MIN_VALUE) {
			return false;
		}
		
		if(region.getLeftChild() == null) {
			return false;
		}
		
		if(region.getRightChild() == null) {
			return false;
		}
		
		if(region.getLeftChild().getState() != DistributionRegionState.ACTIVE) {
			return false;
		}
		
		if(region.getRightChild().getState() != DistributionRegionState.ACTIVE) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Create a new child
	 * @param path
	 * @throws ZookeeperException
	 */
	protected void createNewChild(final String path) throws ZookeeperException {
		logger.debug("Creating: {}", path);

		zookeeperClient.createPersistentNode(path, "".getBytes());
		
		final String distributionGroupName = rootNode.getDistributionGroupName().getFullname();
		final int namePrefix = distributionGroupZookeeperAdapter.getNextTableIdForDistributionGroup(distributionGroupName);
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
				Integer.toString(namePrefix).getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
				"".getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_STATE, 
				DistributionRegionState.CREATING.getStringValue().getBytes());

		distributionGroupZookeeperAdapter.setStateForDistributionGroup(path, DistributionRegionState.ACTIVE);
	}
	
	/**
	 * Read the distribution group in a recursive way
	 * @param path
	 * @param region
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void readDistributionGroupRecursive(final String path, final DistributionRegion region) throws ZookeeperException {
			
			logger.debug("Reading path: {}", path);
			
			try {
				final int regionId = distributionGroupZookeeperAdapter.getRegionIdForPath(path);
				region.setRegionId(regionId);

				// Handle systems and mappings
				updateSystemsForRegion(region);
				
				// If the node is not split, stop recursion
				if(distributionGroupZookeeperAdapter.isGroupSplitted(path)) {
					final float splitFloat = distributionGroupZookeeperAdapter.getSplitPositionForPath(path);
					
					if(region.isLeafRegion()) {
						region.setSplit(splitFloat); 
					} else {
						if(region.getSplit() != splitFloat) {
							logger.warn("Got different split positions: memory {}, zk {} for {}", 
									region.getSplit(), splitFloat, path);
						}
					}
					
					readDistributionGroupRecursive(path + "/" + ZookeeperNodeNames.NAME_LEFT, 
							region.getLeftChild());
					
					readDistributionGroupRecursive(path + "/" + ZookeeperNodeNames.NAME_RIGHT, 
							region.getRightChild());
				}
				
				// Handle state updates at the end.
				// Otherwise, we could set the region to splitted 
				// and the child regions are not ready
				final DistributionRegionState stateForDistributionRegion 
					= distributionGroupZookeeperAdapter.getStateForDistributionRegion(path, this);
				region.setState(stateForDistributionRegion);

			} catch (ZookeeperNotFoundException e) {
				handleRootElementDeleted();
			}
	
			// Wake up all pending waiters
			synchronized (MUTEX) {
				MUTEX.notifyAll();
			}
	}

	/**
	 * Read and update systems for region
	 * @param region
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 */
	protected void updateSystemsForRegion(final DistributionRegion region)
			throws ZookeeperException {
		
		try {
			final Collection<DistributedInstance> systemsForDistributionRegion 
				= distributionGroupZookeeperAdapter.getSystemsForDistributionRegion(region, this);
			
			region.setSystems(systemsForDistributionRegion);
			updateLocalMappings(region, systemsForDistributionRegion);
		} catch (ZookeeperNotFoundException e) {
			removeLocalMappings(region);
		}
	}
	
	/**
	 * Remove the local mappings for a given regions
	 * @param region
	 */
	protected void removeLocalMappings(DistributionRegion region) {
		// Remove the mapping from the nameprefix mapper	
		final int nameprefix = region.getRegionId();		
		logger.info("Remove local mapping for: {} / nameprefix {}", region, nameprefix);
		RegionIdMapperInstanceManager.getInstance(region.getDistributionGroupName()).removeMapping(nameprefix);
	}

	/**
	 * Update the local mappings with the systems for region
	 * @param region
	 * @param systems
	 */
	protected void updateLocalMappings(final DistributionRegion region, 
			final Collection<DistributedInstance> systems) {
		
		if(zookeeperClient.getInstancename() == null) {
			logger.debug("Local instance name is not set, so no local mapping is possible");
			return;
		}
		
		if(systems == null) {
			return;
		}
		
		final DistributedInstance localInstance = zookeeperClient.getInstancename();
		
		// Add the mapping to the nameprefix mapper
		for(final DistributedInstance instance : systems) {
			if(instance.socketAddressEquals(localInstance)) {
				final int nameprefix = region.getRegionId();
				final BoundingBox converingBox = region.getConveringBox();
				
				logger.info("Add local mapping for: {} / nameprefix {}", region, nameprefix);
				RegionIdMapperInstanceManager.getInstance(region.getDistributionGroupName()).addMapping(nameprefix, converingBox);
			}
		}
	}
}

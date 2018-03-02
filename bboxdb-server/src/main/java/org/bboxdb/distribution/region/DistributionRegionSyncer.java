/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.distribution.region;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.DistributionRegionChangedCallback;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionSyncer implements Watcher {
	
	/**
	 * All nodes in zookeeper have versions
	 */
	private final Map<DistributionRegion, Long> versions;
	
	/**
	 * The root node of the K-D-Tree
	 */
	private DistributionRegion rootNode;
	/** 
	 * The region mapper
	 */
	private final DistributionRegionIdMapper distributionRegionMapper;

	/**
	 * The distribution group name
	 */
	private DistributionGroupName distributionGroupName;
	
	/**
	 * The distribution group adapter
	 */
	private DistributionGroupZookeeperAdapter distributionGroupAdapter;
	
	/**
	 * The callbacks
	 */
	private final Set<DistributionRegionChangedCallback> callbacks;

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionSyncer.class);

	public DistributionRegionSyncer(final DistributionGroupName distributionGroupName, 
			final DistributionGroupZookeeperAdapter distributionGroupAdapter, 
			final DistributionRegionIdMapper distributionRegionMapper, 
			final Set<DistributionRegionChangedCallback> callbacks) {
		
		this.distributionGroupName = distributionGroupName;
		this.distributionGroupAdapter = distributionGroupAdapter;
		this.versions = new HashMap<>();
		this.distributionRegionMapper = distributionRegionMapper;
		this.callbacks = callbacks;
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
	}
	
	/**
	 * The zookeeper callbacks
	 */
	@Override
	public void process(final WatchedEvent event) {
		try {
			// Ignore events like connected and disconnected
			if(event == null || event.getPath() == null) {
				return;
			}
			
			if(! event.getPath().endsWith(ZookeeperNodeNames.NAME_NODE_VERSION)) {
				logger.debug("Ignoring event: {}", event);
				return;
			}
			
			if(event.getType() == EventType.NodeDeleted) {
				logger.debug("Ignoring deleted event on {}, this will be handled by parent update",
						event.getPath());
				return;
			}
			
			logger.info("Handling event: {}", event);
			
			processNodeUpdateEvent(event);
		} catch (Throwable e) {
			logger.error("Got uncatched throwable during event handling", e);
		}	
	}

	/**
	 * Process a node update event
	 * @param event
	 */
	private void processNodeUpdateEvent(final WatchedEvent event) {
		final String eventPath = event.getPath();
		final String nodePath = eventPath.replace("/" + ZookeeperNodeNames.NAME_NODE_VERSION, "");
		
		final DistributionRegion region = distributionGroupAdapter.getNodeForPath(rootNode, nodePath);
		
		updateNodeIfNeeded(nodePath, region);
	}

	/**
	 * Update the given node as needed
	 * @param nodePath
	 * @param region
	 */
	private void updateNodeIfNeeded(final String nodePath, final DistributionRegion region) {
		try {
			logger.info("updateNodeIfNeeded called with path {}", nodePath);
						
			final long remoteVersion = distributionGroupAdapter.getNodeMutationVersion(nodePath, this);
			final long localVersion = versions.getOrDefault(region, 0l);
			
			if(localVersion > remoteVersion) {
				logger.error("Local version {} for {} is newer than remote version {}", 
						localVersion, nodePath, remoteVersion);
				return;
			}
			
			if(remoteVersion == localVersion) {
				logger.info("Ignoring event for {}, version has not changed", nodePath);
				return;
			}
			
			logger.info("Updating node {} (local {} / remote {})", 
					nodePath, localVersion, remoteVersion);
			
			updateNode(nodePath, region);
			versions.put(region, remoteVersion);
			notifyCallbacks(region);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			logger.error("Got exception while handling zookeeper callback");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}
	
	/**
	 * Update the given node
	 * @param nodePath
	 * @param region
	 * @throws InterruptedException 
	 */
	private void updateNode(final String nodePath, final DistributionRegion region) 
			throws InterruptedException {
		
		logger.info("updateNode called with node {}", nodePath);
		
		boolean updateSuccessfully = true;
		
		do {
			updateSuccessfully = true;
			
			try {
				final Collection<BBoxDBInstance> systemsForDistributionRegion 
					= distributionGroupAdapter.getSystemsForDistributionRegion(region);
	
				region.setSystems(systemsForDistributionRegion);
				
				final int regionId = distributionGroupAdapter.getRegionIdForPath(nodePath);
				
				if(region.getRegionId() != regionId) {
					throw new RuntimeException("Replacing region id " + region.getRegionId() 
						+ " with " + regionId + " on " + nodePath);
				}
				
				final DistributionRegionState regionState 
					= distributionGroupAdapter.getStateForDistributionRegion(nodePath, this);
			
				region.setState(regionState);
			
				updateChildrenForRegion(nodePath, region);
				
			} catch (ZookeeperException | ZookeeperNotFoundException e) {
				logger.info("Got error while rereading tree, retry", e);
				Thread.sleep(100);
				updateSuccessfully = false;
			} 
	
		} while(updateSuccessfully == false);

	}
	
	/**
	 * Read split position and read children
	 * @param path
	 * @param region
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private void updateChildrenForRegion(final String path, final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final List<String> children = zookeeperClient.getChildren(path);
		final List<Long> registeredChildren = region.getAllChildrenNumbers();
		
		// Process all registered children
		for(final String child : children) {
			if(! child.startsWith(ZookeeperNodeNames.NAME_CHILDREN)) {
				continue;
			}
			
			final String childPath = path + "/" + child;
			logger.info("Reading {}", childPath);
			
			if(! distributionGroupAdapter.isNodeCompletelyCreated(childPath)) {
				logger.info("Node {} not complete, skipping", childPath);
				continue;
			} 
			
			final String[] split = child.split("-");
			final int childNumber = Integer.parseInt(split[1]);
			
			// Mark as seen
			registeredChildren.removeIf((c) -> c == childNumber);
			
			if(region.getChildNumber(childNumber) == null) {
				final DistributionRegion newChild = readChild(childPath, region);
				region.addChildren(childNumber, newChild);
				updateNodeIfNeeded(childPath, newChild);
			}
		}
		
		// Process all in-memory children which are not registered in zookeeper
		if(registeredChildren.isEmpty() == false) {
			logger.info("Removing not existing children {}", registeredChildren);
			registeredChildren.forEach((number) -> region.removeChildren(number)); 
		}
	}
	
	/**
	 * Read the child from the path
	 * @param childPath
	 * @param parentRegion
	 * @return
	 * @throws ZookeeperNotFoundException 
	 * @throws ZookeeperException 
	 */
	private DistributionRegion readChild(final String childPath, final DistributionRegion parentRegion) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final long remoteVersion = distributionGroupAdapter.getNodeMutationVersion(childPath, this);
		final BoundingBox boundingBox = distributionGroupAdapter.getBoundingBoxForPath(childPath);
		final long regionId = distributionGroupAdapter.getRegionIdForPath(childPath);
		
		final DistributionRegion region = new DistributionRegion(distributionGroupName, parentRegion, boundingBox, regionId);		
	
		versions.put(region, remoteVersion);
		notifyCallbacks(region);
		
		return region;
	}
	
	/**
	 * Update the local mappings with the systems for region
	 * @param region
	 * @param systems
	 */
	public void updateLocalMappings() {
				
		if(rootNode == null || distributionGroupName == null) {
			logger.debug("Root node is {}, distributionGroupNameIs {}", rootNode, distributionGroupName);
			return;
		}
		
		final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();

		if(localInstance == null) {
			logger.debug("Local instance name is not set, so no local mapping is possible");
			return;
		}
		
		final List<DistributionRegion> allChildren = rootNode.getThisAndChildRegions();
		
		final Set<Long> allExistingMappings = new HashSet<>(distributionRegionMapper.getAllRegionIds());
		
		final List<DistributionRegionState> activeStates = 
				Arrays.asList(DistributionRegionState.ACTIVE, DistributionRegionState.ACTIVE_FULL);
				
		final String fullname = distributionGroupName.getFullname();
		
		for(final DistributionRegion region : allChildren) {
			final long regionId = region.getRegionId();
			logger.debug("Processing region {}", regionId);
			
			if(! region.getSystems().contains(localInstance)) {
				continue;
			}
			
			if(activeStates.contains(region.getState())) {
				
				// Add the mapping to the nameprefix mapper
				if(! allExistingMappings.contains(regionId)) {
					distributionRegionMapper.addMapping(regionId, region.getConveringBox(), fullname);
				}
				
				allExistingMappings.remove(regionId);
			}	
		}
	
		// Remove all active but not seen mappings
		allExistingMappings.forEach(m -> distributionRegionMapper.removeMapping(m, fullname));
	}
	
	/**
	 * Get the root node
	 * @return
	 */
	public DistributionRegion getRootNode() {
		
		// Reload root node
		if(rootNode == null) {
			final String path = distributionGroupAdapter.getDistributionGroupPath(distributionGroupName.toString());
			
			try {
				if(distributionGroupAdapter.isNodeCompletelyCreated(path)) {
					logger.info("Create new root element for {}", distributionGroupName);
					
					final BoundingBox rootBoundingBox = distributionGroupAdapter.getBoundingBoxForPath(path);
					
					rootNode = new DistributionRegion(distributionGroupName, rootBoundingBox);
					updateNodeIfNeeded(path, rootNode);
				} else {
					logger.info("Root node does not exist");
				}
			} catch (ZookeeperException | ZookeeperNotFoundException e) {
				logger.info("Got exception while reading root node", e);
			}
		}
		
		return rootNode;
	}
	
	/**
	 * Get the region mapper
	 * @return
	 */
	public DistributionRegionIdMapper getDistributionRegionMapper() {
		return distributionRegionMapper;
	}
	
	/**
	 * Register a new callback
	 * @param callback
	 * @return
	 */
	public boolean registerCallback(final DistributionRegionChangedCallback callback) {
		return callbacks.add(callback);
	}
	
	/**
	 * Unregister a callback
	 * @param callback
	 * @return 
	 */
	public boolean unregisterCallback(final DistributionRegionChangedCallback callback) {
		return callbacks.remove(callback);
	}
	
	/**
	 * Notify the callbacks
	 * @param region
	 */
	private void notifyCallbacks(final DistributionRegion region) {
		callbacks.forEach(c -> c.regionChanged(region));
	}
}

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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.bboxdb.commons.Retryer;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitionerContext;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.NodeMutationHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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
	private String distributionGroupName;
	
	/**
	 * The distribution group adapter
	 */
	private DistributionRegionAdapter distributionRegionAdapter;
	
	/**
	 * The distribution group adapter
	 */
	private DistributionGroupAdapter distributionGroupAdapter;
	
	/**
	 * The callbacks
	 */
	private final Set<DistributionRegionCallback> callbacks;

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributionRegionSyncer.class);

	public DistributionRegionSyncer(final SpacePartitionerContext spacePartitionerContext) {

		final ZookeeperClient zookeeperClient = spacePartitionerContext.getZookeeperClient();

		this.distributionGroupName = spacePartitionerContext.getDistributionGroupName();
		this.distributionRegionAdapter = zookeeperClient.getDistributionRegionAdapter();
		this.distributionGroupAdapter = zookeeperClient.getDistributionGroupAdapter();
		this.distributionRegionMapper = spacePartitionerContext.getDistributionRegionMapper();
		this.callbacks = spacePartitionerContext.getCallbacks();
		this.zookeeperClient = zookeeperClient;
		this.versions = new HashMap<>();
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
				processNodeDeletedEvent(event);
				return;
			}
			
			logger.debug("Handling event: {}", event);
			processNodeUpdateEvent(event);
		} catch (Throwable e) {
			logger.error("Got uncatched throwable during event handling", e);
		}	
	}

	/**
	 * Process the node deleted event. This is needed because when the whole
	 * tree is deleted, the parent nodes are not marked as mutated.
	 * 
	 * @param event
	 */
	private void processNodeDeletedEvent(WatchedEvent event) {
		final String eventPath = event.getPath();
		final String nodePath = eventPath.replace("/" + ZookeeperNodeNames.NAME_NODE_VERSION, "");
		
		final DistributionRegion region = distributionRegionAdapter.getNodeForPath(rootNode, nodePath);
		
		if(region == null) {
			return;
		}
		
		if(region.isRootElement()) {
			notifyCallbacks(DistributionRegionEvent.REMOVED, region);
		} else {
			removeChild(region);
		}
	}

	/**
	 * Check for deleted children
	 * @param region
	 */
	private void removeChild(final DistributionRegion region) {
		final DistributionRegion parentRegion = region.getParent();
		final long regionNumber = region.getChildNumberOfParent();
		region.removeChildren(regionNumber);
		notifyCallbacks(DistributionRegionEvent.REMOVED, region);
		notifyCallbacks(DistributionRegionEvent.CHANGED, parentRegion);
	}

	/**
	 * Process a node update event
	 * @param event
	 */
	private void processNodeUpdateEvent(final WatchedEvent event) {
		final String eventPath = event.getPath();
		final String nodePath = eventPath.replace("/" + ZookeeperNodeNames.NAME_NODE_VERSION, "");
		
		final DistributionRegion region = distributionRegionAdapter.getNodeForPath(rootNode, nodePath);
		
		if(region == null) {
			logger.debug("Got null region when reading path {}, waiting for node deletion", nodePath);
			return;
		}
		
		updateNodeIfNeeded(nodePath, region);
	}

	/**
	 * Update the given node as needed
	 * @param nodePath
	 * @param region
	 */
	private void updateNodeIfNeeded(final String nodePath, final DistributionRegion region) {
		try {
			logger.debug("updateNodeIfNeeded called with path {}", nodePath);
						
			final long remoteVersion = NodeMutationHelper.getNodeMutationVersion(
					zookeeperClient, nodePath, this);
			
			final long localVersion = versions.getOrDefault(region, 0l);
			
			if(localVersion > remoteVersion) {
				logger.error("Local version {} for {} is newer than remote version {}", 
						localVersion, nodePath, remoteVersion);
				return;
			}
			
			if(remoteVersion == localVersion) {
				logger.debug("Ignoring event for {}, version has not changed", nodePath);
				return;
			}
			
			logger.debug("Updating node {} (local {} / remote {})", 
					nodePath, localVersion, remoteVersion);
			
			updateNode(nodePath, region);
			versions.put(region, remoteVersion);
			notifyCallbacks(DistributionRegionEvent.CHANGED, region);
			updateLocalMappings();
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
		
		logger.debug("updateNode called with node {}", nodePath);
				
		final Watcher callbackWatcher = this;
		
		final Retryer<Boolean> retryer = new Retryer<>(10, 100, TimeUnit.MILLISECONDS, () -> {
			try {
				final Collection<BBoxDBInstance> systemsForDistributionRegion 
					= distributionRegionAdapter.getSystemsForDistributionRegion(region);
	
				region.setSystems(systemsForDistributionRegion);
				
				final int regionId = distributionGroupAdapter.getRegionIdForPath(nodePath);
				
				if(region.getRegionId() != regionId) {
					throw new RuntimeException("Replacing region id " + region.getRegionId() 
						+ " with " + regionId + " on " + nodePath);
				}
				
				final DistributionRegionState regionState 
					= distributionRegionAdapter.getStateForDistributionRegion(nodePath, callbackWatcher);
			
				region.setState(regionState);
			
				updateChildrenForRegion(nodePath, region);
				
			} catch(ZookeeperNotFoundException e) {
				// Node is deleted, let the deletion callback remove the node
				logger.debug("Skippping node update for path {}, node is deleted", nodePath);
			}
			
			return true;
		});
		
		retryer.execute();
		
		if(! retryer.isSuccessfully()) {
			logger.error("Got error while rereading tree", retryer.getLastException());
		}
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
			logger.debug("Reading {}", childPath);
			
			if(! NodeMutationHelper.isNodeCompletelyCreated(zookeeperClient, childPath)) {
				logger.debug("Node {} not complete, skipping", childPath);
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
				notifyCallbacks(DistributionRegionEvent.ADDED, newChild);
			}
		}
		
		deleteRemovedChildren(region, registeredChildren);
	}

	/**
	 * Delete the not found children
	 * @param region
	 * @param notFoundChildren
	 */
	private void deleteRemovedChildren(final DistributionRegion region, final List<Long> notFoundChildren) {
		
		// Process all in-memory children which are not registered in zookeeper
		for(final long regionNumber : notFoundChildren) {
			logger.debug("Removing not existing children {}", regionNumber);
			final DistributionRegion removedRegion = region.removeChildren(regionNumber);
			notifyCallbacks(DistributionRegionEvent.REMOVED, removedRegion);
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
		
		final Hyperrectangle boundingBox = distributionRegionAdapter.getBoundingBoxForPath(childPath);
		final long regionId = distributionGroupAdapter.getRegionIdForPath(childPath);
		
		final DistributionRegion region = new DistributionRegion(distributionGroupName, parentRegion, boundingBox, regionId);		
			
		return region;
	}
	
	/**
	 * Update the local mappings with the systems for region
	 * @param region
	 * @param systems
	 */
	private void updateLocalMappings() {
				
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
		
		for(final DistributionRegion region : allChildren) {
			final long regionId = region.getRegionId();
			logger.debug("Processing region {}", regionId);
			
			if(! region.getSystems().contains(localInstance)) {
				continue;
			}
			
			if(DistributionRegionHelper.STATES_WRITE.contains(region.getState())) {
				// Add the mapping to the nameprefix mapper
				if(! allExistingMappings.contains(regionId)) {
					distributionRegionMapper.addMapping(regionId, region.getConveringBox());
				}
				
				allExistingMappings.remove(regionId);
			}	
		}
	
		// Remove all active but not seen mappings
		allExistingMappings.forEach(m -> distributionRegionMapper.removeMapping(m));
	}
	
	/**
	 * Get the root node
	 * @return
	 */
	public DistributionRegion getRootNode() {
		
		// Reload root node
		if(rootNode == null) {
			final String groupNameString = distributionGroupName.toString();
			final String path = distributionGroupAdapter.getDistributionGroupRootElementPath(groupNameString);
			
			try {
				if(NodeMutationHelper.isNodeCompletelyCreated(zookeeperClient, path)) {
					logger.info("Create new root element for {}", distributionGroupName);
					
					final Hyperrectangle rootBoundingBox = distributionRegionAdapter.getBoundingBoxForPath(path);
					
					rootNode = new DistributionRegion(distributionGroupName, rootBoundingBox);
					updateNodeIfNeeded(path, rootNode);
				} else {
					logger.info("Root node does not exist");
				}
			} catch (ZookeeperException | ZookeeperNotFoundException e) {
				logger.debug("Got exception while reading root node", e);
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
	public boolean registerCallback(final DistributionRegionCallback callback) {
		return callbacks.add(callback);
	}
	
	/**
	 * Unregister a callback
	 * @param callback
	 * @return 
	 */
	public boolean unregisterCallback(final DistributionRegionCallback callback) {
		return callbacks.remove(callback);
	}
	
	/**
	 * Notify the callbacks
	 * @param region
	 */
	private void notifyCallbacks(final DistributionRegionEvent event, final DistributionRegion region) {
		
		if(region == null) {
			return;
		}
		
		callbacks.forEach(c -> c.regionChanged(event, region));
	}
	
	/**
	 * Clear in memory data
	 */
	@VisibleForTesting
	public void clear() {
		rootNode = null;
		versions.clear();
	}
}

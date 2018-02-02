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
package org.bboxdb.distribution.partitioner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.RegionIdMapper;
import org.bboxdb.distribution.RegionIdMapperInstanceManager;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.regionsplit.SamplingBasedSplitStrategy;
import org.bboxdb.distribution.partitioner.regionsplit.SplitpointStrategy;
import org.bboxdb.distribution.placement.ResourceAllocationException;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KDtreeSpacePartitioner implements Watcher, SpacePartitioner {
	
	/**
	 * The zookeeper client
	 */
	private ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	private DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The name of the distribution group
	 */
	private DistributionGroupName distributionGroupName;
	
	/**
	 * The root node of the K-D-Tree
	 */
	private DistributionRegion rootNode;
	
	/**
	 * The version of the distribution group
	 */
	private String version;
	
	/**
	 * The callbacls
	 */
	private final Set<DistributionRegionChangedCallback> callbacks;
	
	/**
	 * The mutex for sync operations
	 */
	private final Object MUTEX = new Object();

	/**
	 * The space partitioner configuration
	 */
	protected String spacePartitionerConfig;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(KDtreeSpacePartitioner.class);

	public KDtreeSpacePartitioner() {
		this.callbacks = new HashSet<>();		
	}
	
	/**
	 * Reread and handle the dgroup version
	 * @param distributionGroupName
	 * @throws ZookeeperException
	 */
	@Override
	public void init(final String spacePartitionerConfig, 
			final DistributionGroupName distributionGroupName, 
			final ZookeeperClient zookeeperClient, 
			final DistributionGroupZookeeperAdapter distributionGroupAdapter) throws ZookeeperException {
		
		this.spacePartitionerConfig = spacePartitionerConfig;
		this.distributionGroupName = distributionGroupName;
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupZookeeperAdapter = distributionGroupAdapter;
		
		refreshWholeTree();
	}
	
	/**
	 * Register a listener for distribution group changes
	 */
	private void registerDistributionGroupChangeListener() {
		try {
			final List<DistributionGroupName> distributionGroups = distributionGroupZookeeperAdapter.getDistributionGroups(this);
			
			// Is group already in creation?
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
	private void waitForGroupToAppear() throws ZookeeperException{
		final String dgroupPath = distributionGroupZookeeperAdapter.getDistributionGroupPath(distributionGroupName.getFullname());
		
		// Wait for state node to appear
		for(int retry = 0; retry < 10; retry++) {
			try {
				distributionGroupZookeeperAdapter.getStateForDistributionRegion(dgroupPath);
				refreshWholeTree();
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
			logger.error("DGroup {} was created but state field does not appear...", distributionGroupName);
		}
	}
	
	/**
	 * Reread and handle the dgroup version
	 */
	private void refreshWholeTreeNE() {
		try {
			refreshWholeTree();
		} catch (ZookeeperException e) {
			logger.warn("Got zookeeper exception", e);
		}
	}
	
	/**
	 * Refresh the whole tree
	 * @throws ZookeeperException 
	 */
	private void refreshWholeTree() throws ZookeeperException {
		
		try {
			final String zookeeperVersion 
				= distributionGroupZookeeperAdapter.getVersionForDistributionGroup(
					distributionGroupName.getFullname(), this);
			
			if(version == null || ! version.equals(zookeeperVersion)) {
				logger.info("Our tree version is {}, zookeeper version is {}", version, zookeeperVersion);
				version = zookeeperVersion;
				rootNode = null;
				handleRootNodeChanged();
			} 
			
		} catch (ZookeeperNotFoundException e) {
			logger.info("Version for {} not found, deleting in memory version", distributionGroupName);
			version = null;
			rootNode = null;
			
			// The distribution group listener will notify us,
			// as soon as the distribution group is recreated
			handleRootNodeChanged();
			registerDistributionGroupChangeListener();

			return;
		}

		if(rootNode == null) {
			logger.info("Create new root element for {}", distributionGroupName);
			rootNode = DistributionRegion.createRootElement(distributionGroupName);
		}
		
		final String fullname = distributionGroupName.getFullname();
		final String path = distributionGroupZookeeperAdapter.getDistributionGroupPath(fullname);
			
		readDistributionGroupRecursive(path, rootNode);
	}

	/**
	 * Root node has changed, clear local mapppings 
	 */
	private void handleRootNodeChanged() {
		if(rootNode == null) {
			logger.info("Root element for {} is deleted", distributionGroupName);
			RegionIdMapperInstanceManager.getInstance(distributionGroupName).clear();
			TupleStoreConfigurationCache.getInstance().clear();
			DistributionGroupConfigurationCache.getInstance().clear();
		}
	}
	
	/**
	 * Get the root node
	 * @return
	 */
	@Override
	public DistributionRegion getRootNode() {
		synchronized (MUTEX) {
			while(rootNode == null) {
				try {
					MUTEX.wait(30000);
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
			refreshWholeTreeNE();
		} else if(path.endsWith(ZookeeperNodeNames.NAME_SYSTEMS)) {
			// Some systems were added or deleted
			handleSystemNodeUpdateEvent(event);
		} else if(path.endsWith(ZookeeperNodeNames.NAME_SYSTEMS_STATE)) {
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
	private void handleNodeUpdateEvent(final WatchedEvent event) {
	
		if(rootNode == null) {
			logger.debug("Ignore systems update event, because root not node is null: {}", distributionGroupName);
			return;
		}
		
		// Remove state node from path
		final String path = event.getPath().replace("/" + ZookeeperNodeNames.NAME_SYSTEMS_STATE, "");
		
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
	private void handleSystemNodeUpdateEvent(final WatchedEvent event) {
		
		if(rootNode == null) {
			logger.debug("Ignore systems update event, because root not node is null: {}", distributionGroupName);
			return;
		}
		
		final String path = event.getPath().replace("/" + ZookeeperNodeNames.NAME_SYSTEMS, "");
		
		final DistributionRegion nodeToUpdate = distributionGroupZookeeperAdapter.getNodeForPath(rootNode, path);
		
		try {
			if(nodeToUpdate != null) {
				updateSystemsForRegion(nodeToUpdate);
			}
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
	 * @throws ZookeeperNotFoundException 
	 */
	@Override
	public void splitRegion(final DistributionRegion regionToSplit, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) 
			throws BBoxDBException {
		
		try {
			final SplitpointStrategy splitpointStrategy = new SamplingBasedSplitStrategy(tupleStoreManagerRegistry);
			final double splitPosition = splitpointStrategy.getSplitPoint(regionToSplit);
			
			splitNode(regionToSplit, splitPosition);
		} catch (Exception e) {
			throw new BBoxDBException(e);
		} 
	}

	/**
	 * Split the node at the given split point
	 * @param regionToSplit
	 * @param splitPosition
	 * @throws BBoxDBException
	 * @throws ResourceAllocationException 
	 */
	public void splitNode(final DistributionRegion regionToSplit, final double splitPosition)
			throws BBoxDBException, ResourceAllocationException {
		
		try {
			logger.debug("Write split at pos {} into zookeeper", splitPosition);
			final String zookeeperPath = distributionGroupZookeeperAdapter.getZookeeperPathForDistributionRegion(regionToSplit);
			
			final String leftPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_LEFT;
			createNewChild(leftPath);
			
			final String rightPath = zookeeperPath + "/" + ZookeeperNodeNames.NAME_RIGHT;
			createNewChild(rightPath);
			
			// Write split position
			distributionGroupZookeeperAdapter.setSplitPositionForPath(zookeeperPath, splitPosition);
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPath, DistributionRegionState.SPLITTING);
			
			waitUntilChildIsCreated(regionToSplit);
	
			// Allocate systems (the data of the left node is stored locally)
			SpacePartitionerHelper.copySystemsToRegion(regionToSplit, regionToSplit.getLeftChild(), 
					this, distributionGroupZookeeperAdapter);
			
			SpacePartitionerHelper.allocateSystemsToRegion(regionToSplit.getRightChild(), 
					this, distributionGroupZookeeperAdapter);
			
			// update state
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(leftPath, DistributionRegionState.ACTIVE);
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(rightPath, DistributionRegionState.ACTIVE);	
	
			waitForSplitZookeeperCallback(regionToSplit);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			throw new BBoxDBException(e);
		} 
	}
	
	@Override
	public boolean isMergingSupported() {
		return true;
	}

	@Override
	public void prepareMerge(final DistributionRegion regionToMerge) throws BBoxDBException {
		
		try {
			logger.debug("Merging region: {}", regionToMerge);
			final String zookeeperPath = distributionGroupZookeeperAdapter
					.getZookeeperPathForDistributionRegion(regionToMerge);
			
			distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPath, 
					DistributionRegionState.ACTIVE);

			final List<DistributionRegion> childRegions = Arrays.asList(regionToMerge.getLeftChild(), 
					regionToMerge.getRightChild());
			
			for(final DistributionRegion childRegion : childRegions) {
				final String zookeeperPathChild = distributionGroupZookeeperAdapter
						.getZookeeperPathForDistributionRegion(childRegion);
				
				distributionGroupZookeeperAdapter.setStateForDistributionGroup(zookeeperPathChild, 
					DistributionRegionState.MERGING);
			}
			
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	@Override
	public void mergeComplete(final DistributionRegion regionToMerge) throws BBoxDBException {
		final List<DistributionRegion> childRegions = Arrays.asList(regionToMerge.getLeftChild(), 
				regionToMerge.getRightChild());
		
		for(final DistributionRegion childRegion : childRegions) {
			deleteChild(childRegion);
		}
	}

	/**
	 * Wait for zookeeper split callback
	 * @param regionToSplit
	 */
	private void waitUntilChildIsCreated(final DistributionRegion regionToSplit) {
		
		// Wait for zookeeper callback
		synchronized (MUTEX) {
			while(! regionToSplit.isChildNodesInCreatingState()) {
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
	private void waitForSplitZookeeperCallback(final DistributionRegion regionToSplit) {
		
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
	 * Allocate the given list of systems to a region
	 * @param region
	 * @param allocationSystems
	 * @throws ZookeeperException
	 */
	@Override
	public void allocateSystemsToRegion(final DistributionRegion region, final Set<BBoxDBInstance> allocationSystems)
			throws ZookeeperException {
		
		logger.info("Allocating region {} to {}", region.getIdentifier(), allocationSystems);
		
		// Resource allocation successfully, write data to zookeeper
		for(final BBoxDBInstance instance : allocationSystems) {
			distributionGroupZookeeperAdapter.addSystemToDistributionRegion(region, instance);
		}
	}
	
	/**
	 * Is the split for the given node complete?
	 * @param region
	 * @return
	 */
	private boolean isSplitForNodeComplete(final DistributionRegion region) {
		
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
	private void createNewChild(final String path) throws ZookeeperException {
		logger.debug("Creating: {}", path);

		zookeeperClient.createPersistentNode(path, "".getBytes());
		
		final String distributionGroupName = rootNode.getDistributionGroupName().getFullname();
		final int namePrefix = distributionGroupZookeeperAdapter.getNextTableIdForDistributionGroup(distributionGroupName);
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_NAMEPREFIX, 
				Integer.toString(namePrefix).getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_SYSTEMS, 
				"".getBytes());
		
		zookeeperClient.createPersistentNode(path + "/" + ZookeeperNodeNames.NAME_SYSTEMS_STATE, 
				DistributionRegionState.CREATING.getStringValue().getBytes());
	}
	
	/**
	 * Delete the given child
	 * @param region
	 * @throws BBoxDBException 
	 * @throws ZookeeperException 
	 */
	private void deleteChild(final DistributionRegion region) throws BBoxDBException {
		if(region.getState() != DistributionRegionState.MERGING) {
			throw new BBoxDBException("State for region is not merging: " + region);
		}
		
		try {
			final String zookeeperPath = distributionGroupZookeeperAdapter
					.getZookeeperPathForDistributionRegion(region);
			
			zookeeperClient.deleteNodesRecursive(zookeeperPath);
		} catch (ZookeeperException e) {
			throw new BBoxDBException(e);
		}
	}
	
	/**
	 * Read the distribution group in a recursive way
	 * @param path
	 * @param region
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	private void readDistributionGroupRecursive(final String path, final DistributionRegion region) throws ZookeeperException {
			
		logger.debug("Reading path: {}", path);
		
		try {
			final DistributionRegionState regionState 
				= distributionGroupZookeeperAdapter.getStateForDistributionRegion(path, this);
			
			// Read region id
			updateIdForRegion(path, region);

			// Handle systems and mappings
			updateSystemsForRegion(region);
			
			// Update split position and read childs
			updateSplitAndChildsForRegion(path, region);
			
			// Handle state updates at the end.
			// Otherwise, we could set the region to splitted 
			// and the child regions are not ready
			region.setState(regionState);

		} catch (ZookeeperNotFoundException e) {
			refreshWholeTree();
		}

		fireDataChanged(region);
	}

	/**
	 * Read split position and read childs
	 * @param path
	 * @param region
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private void updateSplitAndChildsForRegion(final String path, final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		if(! distributionGroupZookeeperAdapter.isGroupSplitted(path)) {
			return;
		}
		
		final double splitFloat = distributionGroupZookeeperAdapter.getSplitPositionForPath(path);
		
		if(! region.hasChilds()) {		
			region.setSplit(splitFloat); 
		} else {
			if(region.getSplit() != splitFloat) {
				logger.error("Got different split positions: memory {}, zk {} for {}", 
						region.getSplit(), splitFloat, path);
			}
		}
		
		final String leftPath = path + "/" + ZookeeperNodeNames.NAME_LEFT;
		if(zookeeperClient.exists(leftPath)) {
			readDistributionGroupRecursive(leftPath, region.getLeftChild());
		} else {
			region.removeChildren();
		}
		
		final String rightPath = path + "/" + ZookeeperNodeNames.NAME_RIGHT;
		if(zookeeperClient.exists(rightPath)) {
			readDistributionGroupRecursive(rightPath, region.getRightChild());
		} else {
			region.removeChildren();
		}
	}

	/**
	 * Read and update region id
	 * @param path
	 * @param region
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private void updateIdForRegion(final String path, final DistributionRegion region) 
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final int regionId = distributionGroupZookeeperAdapter.getRegionIdForPath(path);
		
		if(region.getRegionId() != DistributionRegion.INVALID_REGION_ID) {
			assert (region.getRegionId() == regionId) 
				: "Replacing region id " + region.getRegionId() + " with " + regionId;
		}
		
		region.setRegionId(regionId);
	}

	/**
	 * Fire data changed event
	 * @param region 
	 */
	private void fireDataChanged(final DistributionRegion region) {
		
		// Wake up all pending waiters
		synchronized (MUTEX) {
			MUTEX.notifyAll();
		}
		
		// Notify callbacks
		for(final DistributionRegionChangedCallback callback : callbacks) {
			callback.regionChanged(region);
		}
	}
	
	/**
	 * Register a new callback
	 * @param callback
	 * @return
	 */
	@Override
	public boolean registerCallback(final DistributionRegionChangedCallback callback) {
		return callbacks.add(callback);
	}
	
	/**
	 * Unregister a callback
	 * @param callback
	 * @return 
	 */
	@Override
	public boolean unregisterCallback(final DistributionRegionChangedCallback callback) {
		return callbacks.remove(callback);
	}

	/**
	 * Read and update systems for region
	 * @param region
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException 
	 */
	private void updateSystemsForRegion(final DistributionRegion region)
			throws ZookeeperException {
		
		try {
			final Collection<BBoxDBInstance> systemsForDistributionRegion 
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
	private void removeLocalMappings(final DistributionRegion region) {
		// Remove the mapping from the regionid mapper	
		final long regionId = region.getRegionId();		
		logger.info("Remove local mapping for: {} / nameprefix {}", region, regionId);
		RegionIdMapperInstanceManager.getInstance(region.getDistributionGroupName()).removeMapping(regionId);
	}

	/**
	 * Update the local mappings with the systems for region
	 * @param region
	 * @param systems
	 */
	private void updateLocalMappings(final DistributionRegion region, 
			final Collection<BBoxDBInstance> systems) {
		
		final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();

		if(localInstance == null) {
			logger.debug("Local instance name is not set, so no local mapping is possible");
			return;
		}
		
		if(systems == null) {
			return;
		}
		
		// Don't add local mapping for inactive regions
		if(        region.getState() == DistributionRegionState.SPLITTING
				|| region.getState() == DistributionRegionState.SPLIT) {
			return;
		}
		
		// Add the mapping to the nameprefix mapper
		for(final BBoxDBInstance instance : systems) {
			if(instance.socketAddressEquals(localInstance)) {
				final RegionIdMapper regionIdMapper = RegionIdMapperInstanceManager.getInstance(region.getDistributionGroupName());
				regionIdMapper.addMapping(region);
			}
		}
	}
}

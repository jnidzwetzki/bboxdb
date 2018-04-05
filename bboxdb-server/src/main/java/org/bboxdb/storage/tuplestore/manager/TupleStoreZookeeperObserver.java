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
package org.bboxdb.storage.tuplestore.manager;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.bboxdb.distribution.TupleStoreConfigurationCache;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleStoreZookeeperObserver {
	
	/**
	 * The manager registry
	 */
	private final TupleStoreManagerRegistry registry;

	/**
	 * The known regions
	 */
	private final Set<DistributionRegionEntity> knownRegions;

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;

	/**
	 * The group adapter
	 */
	private final DistributionGroupAdapter groupAdapter;
	
	/**
	 * The tuple store adapter
	 */
	private final TupleStoreAdapter storeAdapter;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreZookeeperObserver.class);
	
	public TupleStoreZookeeperObserver(final TupleStoreManagerRegistry registry) {
		this.registry = registry;
		this.knownRegions = new HashSet<>();
		this.zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		this.groupAdapter = zookeeperClient.getDistributionGroupAdapter();
		this.storeAdapter = zookeeperClient.getTupleStoreAdapter();
		
		readAndWatchTableConfiguration();
	}

	/**
	 * Read table configuraitons
	 * 
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private void readAndWatchTableConfiguration() {
		try {
			final List<String> groups = groupAdapter.getDistributionGroups();
			
			for(final String group : groups) {
				try {
					storeAdapter.getAllTables(group, w -> handleTableDelete(w));
				} catch (Exception e) {
					logger.error("Got exception while reading table configurations", e);
				}
			}
		} catch (Exception e) {
			logger.error("Got exception while reading table configurations", e);
		}
	}
	
	/**
	 * Handle the deleted table event
	 * @param e
	 */
	private void handleTableDelete(final WatchedEvent e) {
				
		if(e.getType() == null) {
			return;
		}
		
		final String path = e.getPath();

		if(path == null) {
			return;
		}
		
		logger.debug("Got event for {}", path);
		
		try {

			final String changedPath = path.replaceFirst(zookeeperClient.getClusterPath() + "/", "");
						
			final String[] splitPath = changedPath.split("/");
			final String distributionGroup = splitPath[0];
			
			// Reregister watcher
			final List<String> allZookeeperTables = storeAdapter.getAllTables(distributionGroup, 
					w -> handleTableDelete(w));
			
			TupleStoreConfigurationCache.getInstance().clear();

			deleteAllDeletedTables(distributionGroup, allZookeeperTables);
		} catch(ZookeeperNotFoundException e1) {
			// Ignore not found exceptions during deletion
			logger.debug("Got exception during delete", e);
		} catch (Throwable e1) {
			logger.error("Got exception while deleting tuple", e1);
		}
	}

	/**
	 * Delete all deleted tables
	 * 
	 * @param distributionGroup
	 * @param allZookeeperTables
	 * @throws BBoxDBException
	 * @throws StorageManagerException
	 */
	private void deleteAllDeletedTables(final String distributionGroup, final List<String> allZookeeperTables)
			throws BBoxDBException, StorageManagerException {
		final List<TupleStoreName> allLocalTables 
			= registry.getAllTablesForDistributionGroup(distributionGroup);
		
		final SpacePartitioner spacePartitioner = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(distributionGroup);
		
		final DistributionRegionIdMapper regionIdMapper = spacePartitioner
				.getDistributionRegionIdMapper();
				
		for(final TupleStoreName localTable : allLocalTables) {
			final String localTableName = localTable.getFullnameWithoutPrefix();
			
			if(! allZookeeperTables.contains(localTableName)) {
				logger.info("Table {} is not known in zookeeper, deleting local", localTableName);
				final Collection<TupleStoreName> localTables = regionIdMapper.getAllLocalTables(localTable);
				
				for(final TupleStoreName ssTableName : localTables) {
					registry.deleteTable(ssTableName, false);	
				}
			}
		}
	}

	/**
	 * Register a new table, a callback is registered on the space partitioner
	 * to delete the table when it is split or merged
	 * 
	 * @param tupleStoreName
	 */
	public void registerTable(final TupleStoreName tupleStoreName) {
		
		if(! tupleStoreName.isDistributedTable()) {
			return; 
		}
		
		final String distributionGroup = tupleStoreName.getDistributionGroup();
		
		final DistributionRegionEntity tableEntity = new DistributionRegionEntity(
				distributionGroup, tupleStoreName.getRegionId().getAsLong());
		
		synchronized (knownRegions) {
			if(knownRegions.contains(tableEntity)) {
				return;
			}
			
			// Register callback
			final DistributionRegionCallback callback = (e, r) -> {
				handleCallback(tableEntity, e, r);
			};
			
			try {
				final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
						.getSpacePartitionerForGroupName(distributionGroup);
				
				spacePartitioner.registerCallback(callback);
				
				knownRegions.add(tableEntity);
				
			} catch (BBoxDBException e) {
				logger.error("Unable to register callback", e);
			}
		}
	}

	/**
	 * Handle the callback and check if local data needs to be deleted.
	 * 
	 * When the region is split, the local data can be removed
	 * When the region is merged and deleted in Zookeeper, the local data can also be removed
	 * 
	 * @param entityToObserver
	 * @param event
	 * @param eventEntity
	 */
	private void handleCallback(final DistributionRegionEntity entityToObserver, 
			final DistributionRegionEvent event, final DistributionRegion eventEntity) {
		
		final String groupName = eventEntity.getDistributionGroupName();
		final DistributionRegionEntity callbackEntity = new DistributionRegionEntity(groupName, eventEntity.getRegionId());
		
		try {
			if(event == DistributionRegionEvent.REMOVED) {
				if(callbackEntity.equals(entityToObserver)) {
					logger.info("The region {} has merged, deleted local data", callbackEntity);
					registry.deleteDataOfDistributionRegion(groupName, eventEntity.getRegionId(), false);
				}
			} else if(eventEntity.getState() == DistributionRegionState.SPLIT) {
				logger.info("The region {} has split, deleted local data", callbackEntity);
				registry.deleteDataOfDistributionRegion(groupName, eventEntity.getRegionId(), false);
			}
			
			storeAdapter.getAllTables(groupName, w -> handleTableDelete(w));

		} catch(ZookeeperNotFoundException e1) {
			logger.info("Got zookeeper not found exception while reading table versions");
		} catch (Throwable e1) {
			logger.error("Got exception while deleting tuple stores", e1);
		}
	}
}

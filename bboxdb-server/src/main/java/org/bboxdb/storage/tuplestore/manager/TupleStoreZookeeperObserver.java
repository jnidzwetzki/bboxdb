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

import java.util.HashSet;
import java.util.Set;

import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
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
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreZookeeperObserver.class);

	
	public TupleStoreZookeeperObserver(final TupleStoreManagerRegistry registry) {
		this.registry = registry;
		this.knownRegions = new HashSet<>();
	}
	
	/**
	 * Register a new table, a callback is registered on the space partitioner
	 * to delete the table when it is split or merged
	 * 
	 * @param tupleStoreName
	 */
	public void registerTable(final TupleStoreName tupleStoreName) {
		synchronized (knownRegions) {
			
			final String distributionGroup = tupleStoreName.getDistributionGroup();
			
			final DistributionRegionEntity tableEntity = new DistributionRegionEntity(
					distributionGroup, tupleStoreName.getRegionId());
			
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
		
		final String groupName = eventEntity.getDistributionGroupName().getFullname();
		final DistributionRegionEntity callbackEntity = new DistributionRegionEntity(groupName, eventEntity.getRegionId());
		
		try {
			if(event == DistributionRegionEvent.REMOVED) {
				if(callbackEntity.equals(entityToObserver)) {
					logger.info("The region {} has merged, deleted local data", callbackEntity);
					registry.deleteDataOfDistributionRegion(groupName, eventEntity.getRegionId());
				}
			} else if(eventEntity.getState() == DistributionRegionState.SPLIT) {
				logger.info("The region {} has split, deleted local data", callbackEntity);
				registry.deleteDataOfDistributionRegion(groupName, eventEntity.getRegionId());
			}
			
		} catch (StorageManagerException e1) {
			logger.error("Got exception while deleting tuple stores", e1);
		}
	}
} 

class DistributionRegionEntity {
	
	/**
	 * The distribution group name
	 */
	private final String distributionGroupName;
	
	/**
	 * The region id
	 */
	private final long regionId;

	public DistributionRegionEntity(final String distributionGroupName, final long regionId) {
		this.distributionGroupName = distributionGroupName;
		this.regionId = regionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionGroupName == null) ? 0 : distributionGroupName.hashCode());
		result = prime * result + (int) (regionId ^ (regionId >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DistributionRegionEntity other = (DistributionRegionEntity) obj;
		if (distributionGroupName == null) {
			if (other.distributionGroupName != null)
				return false;
		} else if (!distributionGroupName.equals(other.distributionGroupName))
			return false;
		if (regionId != other.regionId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DistributionRegionEntity [distributionGroupName=" + distributionGroupName + ", regionId=" + regionId
				+ "]";
	}

	public String getDistributionGroupName() {
		return distributionGroupName;
	}

	public long getRegionId() {
		return regionId;
	}
}

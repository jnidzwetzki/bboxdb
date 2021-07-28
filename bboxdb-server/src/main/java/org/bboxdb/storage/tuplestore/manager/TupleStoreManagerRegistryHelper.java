/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;

public class TupleStoreManagerRegistryHelper {

	/**
	 * Create all missing tables
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	public static void createMissingTables(final TupleStoreName requestTable,
			final TupleStoreManagerRegistry storageRegistry,
			final Collection<TupleStoreName> localTables)
			throws StorageManagerException, BBoxDBException, InterruptedException {

		final boolean unknownTables = localTables.stream()
				.anyMatch((t) -> ! storageRegistry.isStorageManagerKnown(t));

		if(! unknownTables) {
			return;
		}

		// Expensive call (involves Zookeeper interaction)
		try {
			final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
					.getZookeeperClient().getTupleStoreAdapter();

			if(! tupleStoreAdapter.isTableKnown(requestTable)) {
				throw new StorageManagerException("Table: " + requestTable.getFullname() + " is unknown");
			}

			final TupleStoreConfiguration config = tupleStoreAdapter.readTuplestoreConfiguration(requestTable);

			for(final TupleStoreName tupleStoreName : localTables) {
				final boolean alreadyKnown = storageRegistry.isStorageManagerKnown(tupleStoreName);

				if(! alreadyKnown) {
					if(! isDistributionRegionWritable(tupleStoreName)) {
						throw new StorageManagerException("Wrong state to create region " + tupleStoreName);
					}
					
					storageRegistry.createTableIfNotExist(tupleStoreName, config);
				}
			}
		} catch (ZookeeperException e) {
			throw new StorageManagerException(e);
		}
	}
	

	/**
	 * Is the provided region writable
	 * @param tupleStoreName
	 * @return
	 * @throws BBoxDBException
	 * @throws StorageManagerException 
	 * @throws InterruptedException 
	 */
	public static boolean isDistributionRegionWritable(final TupleStoreName tupleStoreName)
			throws BBoxDBException, StorageManagerException, InterruptedException {
		
		final String distributionGroup = tupleStoreName.getDistributionGroup();
		final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroup);

		final DistributionRegion distributionRegion = spacePartitioner.getRootNode();

		final DistributionRegion region = DistributionRegionHelper.getDistributionRegionForNamePrefix(
				distributionRegion, tupleStoreName.getRegionId().getAsLong());
		
		if(region == null) {
			throw new StorageManagerException("Unable to get distribution region for " + tupleStoreName);
		}
		
		final DistributionRegionState regionState = region.getState();
		
		if(! DistributionRegionHelper.PREDICATE_REGIONS_FOR_WRITE.test(regionState)) {
			return false;
		}
		
		return true;
	}
}

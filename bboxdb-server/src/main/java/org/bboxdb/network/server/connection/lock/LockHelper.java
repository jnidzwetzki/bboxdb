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
package org.bboxdb.network.server.connection.lock;

import java.util.Collection;
import java.util.List;

import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockHelper {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(LockHelper.class);

	/**
	 * Remove all locks for the given connection. Might be called after connection shutdown.
	 * 
	 * @param clientConnectionHandler
	 */
	public static void handleLockRemove(final ClientConnectionHandler clientConnectionHandler) {
		final LockManager lockManager = clientConnectionHandler.getLockManager();
		
		final List<LockEntry> removedLocks = lockManager.getAllLocksForObject(clientConnectionHandler);
		
		final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();
		
		for(final LockEntry lockEntry : removedLocks) {
			if(! lockEntry.isDeleteOnTimeout()) {
				continue;
			}
			
			deleteLocalStoredTuple(storageRegistry, lockEntry); 				
		}
		
		// Now the tuple to delete might be cleared and the locks an be removed
		lockManager.removeAllLocksForObject(clientConnectionHandler);
	}

	/**
	 * Delete the local stored tuple
	 * 
	 * @param storageRegistry
	 * @param lockEntry
	 */
	private static void deleteLocalStoredTuple(final TupleStoreManagerRegistry storageRegistry,
			final LockEntry lockEntry) {
		
		logger.info("Connection was terminated deleting {} lock and locally stored data", lockEntry);
		
		try {
			final TupleStoreName tupleStoreName = new TupleStoreName(lockEntry.getTable());
					
			final SpacePartitioner spacePartitioner = SpacePartitionerCache
					.getInstance().getSpacePartitionerForGroupName(tupleStoreName.getDistributionGroup());
			
			final DistributionRegionIdMapper regionIdMapper = spacePartitioner
					.getDistributionRegionIdMapper();
			
			final Collection<TupleStoreName> localTables = regionIdMapper.getAllLocalTables(tupleStoreName);

			for(final TupleStoreName localTable : localTables) {
				final long deletionVersion = lockEntry.getVersion() + 1;
				final String key = lockEntry.getKey();
				
				if(storageRegistry.isStorageManagerKnown(localTable)) {
					storageRegistry.getTupleStoreManager(localTable).delete(key, deletionVersion);
				}
			}
		} catch (Exception e) {
			logger.error("Got exception while deleting tuple", e);
		}
	}
}

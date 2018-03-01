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
package org.bboxdb.distribution;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bboxdb.commons.RejectedException;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceState;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.membership.ZookeeperBBoxDBInstanceAdapter;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.misc.Const;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DistributionGroupMetadata;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.DiskStorage;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedRecoveryService implements BBoxDBService {
	
	/**
	 * The storage registry
	 */
	protected final TupleStoreManagerRegistry storageRegistry;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributedRecoveryService.class);
	
	public DistributedRecoveryService(final TupleStoreManagerRegistry storageRegistry) {
		this.storageRegistry = storageRegistry;
	}

	@Override
	public void init() {
		try {			
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final ZookeeperBBoxDBInstanceAdapter zookeeperBBoxDBInstanceAdapter 
				= new ZookeeperBBoxDBInstanceAdapter(zookeeperClient);
			
			final BBoxDBInstance distributedInstance = ZookeeperClientFactory.getLocalInstanceName();
			zookeeperBBoxDBInstanceAdapter.updateStateData(distributedInstance, BBoxDBInstanceState.OUTDATED);
			
			logger.info("Running recovery for local stored data");
			
			runRecovery();
			
			logger.info("Running recovery for local stored data DONE");
			zookeeperBBoxDBInstanceAdapter.updateStateData(distributedInstance, BBoxDBInstanceState.READY);
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			logger.error("Got an exception during recovery: ", e);
		}
	}

	/**
	 * Run the recovery
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	protected void runRecovery() throws ZookeeperException, ZookeeperNotFoundException {
		
		final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getDistributionGroupAdapter();
		
		final List<DistributionGroupName> distributionGroups 
			= distributionGroupZookeeperAdapter.getDistributionGroups();
		
		for(final DistributionGroupName distributionGroupName : distributionGroups) {
			logger.info("Recovery: running recovery for distribution group: {}", distributionGroupName);
			runRecoveryForDistributionGroup(distributionGroupName);
			logger.info("Recovery: recovery for distribution group done: {}", distributionGroupName);
		}
	}

	/**
	 * Run recovery for distribution group
	 * @param distributionGroupName
	 * @throws ZookeeperException 
	 */
	protected void runRecoveryForDistributionGroup(final DistributionGroupName distributionGroupName) {
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();
			
			for(final DiskStorage storage : storageRegistry.getAllStorages()) {
				checkGroupVersion(storage, distributionGroupName, zookeeperClient);
			}
					
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getSpacePartitionerForGroupName(
					distributionGroupName.getFullname());
			
			final DistributionRegion distributionGroup = spacePartitioner.getRootNode();
		
			final List<OutdatedDistributionRegion> outdatedRegions 
				= DistributionRegionHelper.getOutdatedRegions(distributionGroup, localInstance);
			
			handleOutdatedRegions(distributionGroupName, outdatedRegions);
		} catch (Throwable e) {
			logger.error("Got exception while running recovery for distribution group: " + distributionGroupName, e);
		}
		
	}

	protected void checkGroupVersion(final DiskStorage storage, final DistributionGroupName distributionGroupName,
			final ZookeeperClient zookeeperClient) {
		
		try {
			final DistributionGroupMetadata metaData = DistributionGroupMetadataHelper
					.getMedatadaForGroup(storage.getBasedir().getAbsolutePath(), distributionGroupName);
			
			if(metaData == null) {
				logger.debug("Metadata for storage {} and group {} is null, skipping check", 
						storage.getBasedir(), distributionGroupName.getFullname());
				return;
			}
			
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter 
				= new DistributionGroupZookeeperAdapter(zookeeperClient);
			
			final String remoteVersion = distributionGroupZookeeperAdapter
					.getVersionForDistributionGroup(distributionGroupName.getFullname(), null);
			
			final String localVersion = metaData.getVersion();
			
			if(! remoteVersion.equals(localVersion)) {
				logger.error("Local version {} of dgroup {} does not match remtote version {}", localVersion, distributionGroupName, remoteVersion);
				System.exit(-1);
			}
			
		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			logger.error("Got an exception while checking group version");
		} 
	}

	/**
	 * Handle the outdated distribution regions
	 * @param distributionGroupName
	 * @param outdatedRegions
	 */
	protected void handleOutdatedRegions(final DistributionGroupName distributionGroupName, 
			final List<OutdatedDistributionRegion> outdatedRegions) {
		
		for(final OutdatedDistributionRegion outdatedDistributionRegion : outdatedRegions) {
			
			final BBoxDBClient connection = MembershipConnectionService.getInstance()
					.getConnectionForInstance(outdatedDistributionRegion.getNewestInstance());
			
			final long regionId = outdatedDistributionRegion.getDistributedRegion().getRegionId();
			
			final List<TupleStoreName> allTables = storageRegistry
					.getAllTablesForDistributionGroupAndRegionId(distributionGroupName, regionId);
			
			for(final TupleStoreName ssTableName : allTables) {
				try {
					runRecoveryForTable(ssTableName, outdatedDistributionRegion, connection);
				} catch (RejectedException | StorageManagerException | ExecutionException e) {
					logger.error("Got an exception while performing recovery for table: " + ssTableName.getFullname());
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.error("Got an exception while performing recovery for table: " + ssTableName.getFullname());
				}
			}
		}
	}

	/**
	 * Run the recovery for a given table
	 * @param ssTableName
	 * @param outdatedDistributionRegion
	 * @param connection
	 * @throws StorageManagerException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws RejectedException 
	 */
	protected void runRecoveryForTable(final TupleStoreName ssTableName,
			final OutdatedDistributionRegion outdatedDistributionRegion,
			final BBoxDBClient connection) throws StorageManagerException,
			InterruptedException, ExecutionException, RejectedException {
		
		final String sstableName = ssTableName.getFullname();
		
		logger.info("Recovery: starting recovery for table {}", sstableName);
		final TupleStoreManager tableManager = storageRegistry.getTupleStoreManager(ssTableName);
		
		// Even with NTP, the clock of the nodes can have a delta.
		// We subtract this delta from the checkpoint timestamp to ensure
		// that all tuples for the recovery are requested
		final long requestTupleTimestamp = outdatedDistributionRegion.getLocalVersion() 
				- Const.MAX_NODE_CLOCK_DELTA;
		
		final TupleListFuture result = connection.queryInsertedTime
				(sstableName, requestTupleTimestamp);
		
		result.waitForAll();
		
		if(result.isFailed()) {
			logger.warn("Recovery: Failed result for table {} - Some tuples could not be received!", 
					sstableName);
			return;
		}
		
		long insertedTuples = 0;
		for(final Tuple tuple : result) {
			tableManager.put(tuple);
			insertedTuples++;
		}
		
		logger.info("Recovery: successfully inserted {} tuples into table {}", insertedTuples,
				sstableName);
	}

	@Override
	public void shutdown() {
		// Nothing to do
	}

	@Override
	public String getServicename() {
		return "Recovery service";
	}

}

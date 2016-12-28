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
package org.bboxdb.storage;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.bboxdb.BBoxDBConfiguration;
import org.bboxdb.BBoxDBConfigurationManager;
import org.bboxdb.BBoxDBService;
import org.bboxdb.distribution.DistributionGroupCache;
import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionHelper;
import org.bboxdb.distribution.OutdatedDistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.membership.event.DistributedInstanceState;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.server.NetworkConnectionService;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryService implements BBoxDBService {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RecoveryService.class);
	
	/**
	 * The connection handler
	 */
	protected final NetworkConnectionService connectionHandler;

	public RecoveryService(final NetworkConnectionService connectionHandler) {
		this.connectionHandler = connectionHandler;
	}

	@Override
	public void init() {
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
			zookeeperClient.setLocalInstanceState(DistributedInstanceState.READONLY);
			logger.info("Running recovery for local stored data");
			
			runRecovery();
			
			logger.info("Running recovery for local stored data DONE");
			connectionHandler.setReadonly(false);
			
			zookeeperClient.setLocalInstanceState(DistributedInstanceState.READWRITE);
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
			= distributionGroupZookeeperAdapter.getDistributionGroups(null);
		
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
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClientAndInit();
			final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
			final DistributedInstance localInstance = ZookeeperClientFactory.getLocalInstanceName(configuration);
			
			final KDtreeZookeeperAdapter distributionAdapter = DistributionGroupCache.getGroupForGroupName(
					distributionGroupName.getFullname(), zookeeperClient);
			
			final DistributionRegion distributionGroup = distributionAdapter.getRootNode();
		
			final List<OutdatedDistributionRegion> outdatedRegions = DistributionRegionHelper.getOutdatedRegions(distributionGroup, localInstance);
			handleOutdatedRegions(distributionGroupName, outdatedRegions);
		} catch (ZookeeperException e) {
			logger.error("Got exception while running recovery for distribution group: " + distributionGroupName, e);
		}
		
	}

	/**
	 * Handle the outdated distribution regions
	 * @param distributionGroupName
	 * @param outdatedRegions
	 */
	protected void handleOutdatedRegions(final DistributionGroupName distributionGroupName, final List<OutdatedDistributionRegion> outdatedRegions) {
		for(final OutdatedDistributionRegion outdatedDistributionRegion : outdatedRegions) {
			
			final BBoxDBClient connection = MembershipConnectionService.getInstance()
					.getConnectionForInstance(outdatedDistributionRegion.getNewestInstance());
			
			final List<SSTableName> allTables = StorageRegistry
					.getAllTablesForNameprefix(outdatedDistributionRegion.getDistributedRegion().getRegionId());
			
			for(final SSTableName ssTableName : allTables) {
				try {
					runRecoveryForTable(ssTableName, outdatedDistributionRegion, connection);
				} catch (StorageManagerException | ExecutionException e) {
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
	 */
	protected void runRecoveryForTable(final SSTableName ssTableName,
			final OutdatedDistributionRegion outdatedDistributionRegion,
			final BBoxDBClient connection) throws StorageManagerException,
			InterruptedException, ExecutionException {
		
		logger.info("Recovery: starting recovery for table {}", ssTableName.getFullname());
		final SSTableManager tableManager = StorageRegistry.getSSTableManager(ssTableName);
		final TupleListFuture result = connection.queryTime(ssTableName.getFullname(), outdatedDistributionRegion.getLocalVersion());
		result.waitForAll();
		
		if(result.isFailed()) {
			logger.warn("Recovery: Failed result for table {} - Some tuples could not be received!", 
					ssTableName.getFullname());
			return;
		}
		
		long insertedTuples = 0;
		for(final Tuple tuple : result) {
			tableManager.put(tuple);
			insertedTuples++;
		}
		
		logger.info("Recovery: successfully inserted {} tuples into table {}", insertedTuples, ssTableName.getFullname());
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

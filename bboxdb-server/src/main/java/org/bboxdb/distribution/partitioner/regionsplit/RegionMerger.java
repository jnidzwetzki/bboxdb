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
package org.bboxdb.distribution.partitioner.regionsplit;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBConnection;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.tuplestore.manager.TupleStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionMerger {

	/**
	 * The storage reference
	 */
	private final TupleStoreManagerRegistry registry;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionMerger.class);

	public RegionMerger(final TupleStoreManagerRegistry registry) {
		assert (registry != null) : "Unable to init, registry is null";		
		this.registry = registry;
	}

	/**
	 * Merge the given region
	 * @param region
	 * @param distributionGroupZookeeperAdapter
	 * @param spacePartitioner
	 * @param diskStorage
	 */
	public void mergeRegion(final List<DistributionRegion> source, final SpacePartitioner spacePartitioner, 
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) {

		assert(! source.isEmpty());
		
		logger.info("Performing merge for: {}", source.get(0).getIdentifier());

		final DistributionRegionAdapter distributionGroupZookeeperAdapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();

		DistributionRegion destination = null;
		
		try {
			destination = spacePartitioner.getDestinationForMerge(source);
			
			if(destination == null) {
				logger.error("Got null when calling getDestinationForMerge from space partitoner {}",
						source);
				return;
			}
			
			final String identifier = destination.getIdentifier();
			
			// Try to set region state to merging. If this fails, another node is already 
			// merges the region
			final boolean setToMergeResult = distributionGroupZookeeperAdapter.setToSplitMerging(destination);

			if(! setToMergeResult) {
				logger.info("Unable to set state to split merge for region: {}, stopping merge", identifier);
				logger.info("Old state was {}", distributionGroupZookeeperAdapter.getStateForDistributionRegion(destination));
				return;
			}

			spacePartitioner.prepareMerge(source, destination);
			redistributeDataMerge(source, destination);
			spacePartitioner.mergeComplete(source, destination);

		} catch (Throwable e) {
			logger.warn("Got uncought exception during merge: " + source.get(0).getIdentifier(), e);
			handleMergeFailed(source, destination, spacePartitioner);
		}
	}

	/**
	 * Handle failed merge
	 * @param source 
	 * @param region
	 * @param spacePartitioner
	 * @throws BBoxDBException
	 */
	private void handleMergeFailed(List<DistributionRegion> source, final DistributionRegion region, 
			final SpacePartitioner spacePartitioner) {
		
		try {
			spacePartitioner.mergeFailed(source, region);
		} catch (BBoxDBException e) {
			logger.error("Unable to handle merge failed on: " + region.getIdentifier(), e);
		}
	}

	/**
	 * Redistribute the data in region merge
	 * @param source 
	 * @param destination
	 * @throws Exception
	 */
	private void redistributeDataMerge(final List<DistributionRegion> source, 
			final DistributionRegion destination) throws Exception {

		logger.info("Redistributing all data for region (merge): " + destination.getIdentifier());

		final String distributionGroupName = destination.getDistributionGroupName();

		final List<TupleStoreName> localTables = TupleStoreUtil.getAllTablesForDistributionGroupAndRegionId
				(registry, distributionGroupName, destination.getRegionId());

		// Add the local mapping, new data is written to the region
		final SpacePartitioner spacePartitioner = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(distributionGroupName);
		
		final DistributionRegionIdMapper mapper = spacePartitioner.getDistributionRegionIdMapper();		
		
		// We have set the region to active, wait until we see this status change 
		// from Zookeeper and the space partitioner add this region as active
		mapper.waitUntilMappingAppears(destination.getRegionId());

		// Redistribute data
		for(final TupleStoreName tupleStoreName : localTables) {
			logger.info("Merging data of tuple store {}", tupleStoreName);
			startFlushToDisk(tupleStoreName);

			final TupleRedistributor tupleRedistributor 
				= new TupleRedistributor(registry, tupleStoreName);

			tupleRedistributor.registerRegion(destination);

			for(final DistributionRegion childRegion : source) {
				mergeDataFromChildRegion(destination, tupleStoreName, tupleRedistributor, childRegion);					
			}

			logger.info("Final statistics for merge ({}): {}", 
					tupleStoreName,tupleRedistributor.getStatistics());
		}
	}

	/**
	 * Merge the data from the given child region
	 * 
	 * @param region
	 * @param tupleStoreName
	 * @param tupleRedistributor
	 * @param childRegion
	 * @throws StorageManagerException
	 */
	private void mergeDataFromChildRegion(final DistributionRegion region, 
			final TupleStoreName tupleStoreName,	final TupleRedistributor tupleRedistributor, 
			final DistributionRegion childRegion) throws StorageManagerException {

		try {
			final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();

			if(childRegion.getSystems().contains(localInstance)) {
				mergeDataByLocalRead(region, tupleStoreName, tupleRedistributor, childRegion);
			} else {
				mergeDataByNetworkRead(region, tupleStoreName, tupleRedistributor, childRegion);	
			}

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new StorageManagerException(e);
		} catch (Exception e) {
			throw new StorageManagerException(e);
		}
	}

	/**
	 * Merge data by local data read
	 * 
	 * @param region
	 * @param tupleStoreName
	 * @param tupleRedistributor
	 * @param childRegion
	 * @throws StorageManagerException 
	 * @throws TupleStoreManagerRegistry 
	 */
	private void mergeDataByLocalRead(final DistributionRegion region, final TupleStoreName tupleStoreName,
			final TupleRedistributor tupleRedistributor, final DistributionRegion childRegion) 
			throws StorageManagerException {

		final long regionId = region.getRegionId();
		final TupleStoreName childRegionName = tupleStoreName.cloneWithDifferntRegionId(regionId);
		
		final TupleStoreManager tupleStoreManager = registry.getTupleStoreManager(childRegionName);
		
		final List<ReadOnlyTupleStore> storages = new ArrayList<>();
		
		try {
			storages.addAll(tupleStoreManager.aquireStorage());
			
			for(final ReadOnlyTupleStore storage : storages) {
				for(final Tuple tuple : storage) {
					tupleRedistributor.redistributeTuple(tuple);
				}
			}
		} catch(Exception e) {
			throw e;
		} finally {
			tupleStoreManager.releaseStorage(storages);
		}
	}

	/**
	 * Merge the region by a network read
	 * 
	 * @param region
	 * @param tupleStoreName
	 * @param tupleRedistributor
	 * @param childRegion
	 * @throws InterruptedException
	 * @throws StorageManagerException
	 * @throws Exception
	 */
	private void mergeDataByNetworkRead(final DistributionRegion region, final TupleStoreName tupleStoreName,
			final TupleRedistributor tupleRedistributor, final DistributionRegion childRegion)
					throws InterruptedException, StorageManagerException {

		final List<BBoxDBInstance> systems = childRegion.getSystems();
		assert(! systems.isEmpty()) : "Systems can not be empty";

		final BBoxDBInstance firstSystem = systems.get(0);

		final BBoxDBConnection connection = MembershipConnectionService.getInstance()
				.getConnectionForInstance(firstSystem);

		assert (connection != null) : "Connection can not be null: " + firstSystem.getStringValue();

		final Hyperrectangle bbox = childRegion.getConveringBox();
		final String fullname = tupleStoreName.getFullname();
		final BBoxDBClient bboxDBClient = connection.getBboxDBClient();
		final TupleListFuture result = bboxDBClient.queryRectangle(fullname, bbox);

		result.waitForCompletion();

		if(result.isFailed()) {
			throw new StorageManagerException("Exception while fetching tuples: " 
					+ result.getAllMessages());
		}

		for(final Tuple tuple : result) {
			tupleRedistributor.redistributeTuple(tuple);
		}
	}

	/**
	 * Start the to disk flushing
	 * @param ssTableName
	 * @throws StorageManagerException
	 */
	private void startFlushToDisk(final TupleStoreName ssTableName) throws StorageManagerException {
		final TupleStoreManager ssTableManager = registry.getTupleStoreManager(ssTableName);		
		ssTableManager.init();
		ssTableManager.setToReadWrite();
	}
}

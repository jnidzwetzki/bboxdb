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

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.DistributionRegionIdMapper;
import org.bboxdb.distribution.DistributionRegionIdMapperManager;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
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
	public void mergeRegion(final DistributionRegion region, final SpacePartitioner spacePartitioner,
			final TupleStoreManagerRegistry tupleStoreManagerRegistry) {

		assert(region != null);
		assert(! region.isLeafRegion()) : "Unable to perform merge on: " + region + " is leaf";

		logger.info("Performing merge for: {}", region.getIdentifier());

		final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter = ZookeeperClientFactory.getDistributionGroupAdapter();

		try {
			// Try to set region state to full. If this fails, another node is already 
			// splits the region
			final boolean setToMergeResult = distributionGroupZookeeperAdapter.setToSplitMerging(region);

			if(! setToMergeResult) {
				logger.info("Unable to set state to split merge for region: {}, stopping merge", region.getIdentifier());
				logger.info("Old state was {}", distributionGroupZookeeperAdapter.getStateForDistributionRegion(region));
				return;
			}

			spacePartitioner.prepareMerge(region);
			redistributeDataMerge(region);
			spacePartitioner.mergeComplete(region);

		} catch (Throwable e) {
			logger.warn("Got uncought exception during merge: " + region.getIdentifier(), e);
		}
	}

	/**
	 * Redistribute the data in region merge
	 * @param region
	 * @throws StorageManagerException 
	 * @throws BBoxDBException 
	 */
	private void redistributeDataMerge(final DistributionRegion region) 
			throws StorageManagerException, BBoxDBException {

		logger.info("Redistributing all data for region (merge): " + region.getIdentifier());

		final List<DistributionRegion> childRegions = region.getChildren();

		final DistributionGroupName distributionGroupName = region.getDistributionGroupName();

		final List<TupleStoreName> localTables = registry.getAllTablesForDistributionGroupAndRegionId
				(distributionGroupName, region.getRegionId());

		// Add the local mapping, new data is written to the region
		final DistributionRegionIdMapper mapper = DistributionRegionIdMapperManager.getInstance(distributionGroupName);
		final boolean addResult = mapper.addMapping(region);

		assert (addResult == true) : "Unable to add mapping for: " + region;

		// Redistribute data
		for(final TupleStoreName tupleStoreName : localTables) {
			logger.info("Merging data of tuple store {}", tupleStoreName);
			startFlushToDisk(tupleStoreName);

			final TupleRedistributor tupleRedistributor 
				= new TupleRedistributor(registry, tupleStoreName);

			tupleRedistributor.registerRegion(region);

			for(final DistributionRegion childRegion : childRegions) {
				mergeDataFromChildRegion(region, tupleStoreName, tupleRedistributor, childRegion);					
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

		final BBoxDBClient connection = MembershipConnectionService.getInstance()
				.getConnectionForInstance(firstSystem);

		assert (connection != null) : "Connection can not be null: " + firstSystem.getStringValue();

		final BoundingBox bbox = childRegion.getConveringBox();
		final String fullname = tupleStoreName.getFullname();
		final TupleListFuture result = connection.queryBoundingBox(fullname, bbox);

		result.waitForAll();

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

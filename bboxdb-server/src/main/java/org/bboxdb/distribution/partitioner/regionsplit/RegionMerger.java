/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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

import java.util.List;
import java.util.function.Consumer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor.ExecutionPolicy;
import org.bboxdb.distribution.partitioner.regionsplit.tuplesink.TupleRedistributor;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.server.query.QueryHelper;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
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

		// Add the local mapping, new data is written to the region
		final SpacePartitioner spacePartitioner = SpacePartitionerCache
				.getInstance().getSpacePartitionerForGroupName(distributionGroupName);
		
		final DistributionRegionIdMapper mapper = spacePartitioner.getDistributionRegionIdMapper();		
		
		// We have set the region to active, wait until we see this status change 
		// from Zookeeper and the space partitioner add this region as active
		mapper.waitUntilMappingAppears(destination.getRegionId());
		
		final TupleStoreAdapter adapter = new TupleStoreAdapter(ZookeeperClientFactory.getZookeeperClient());
		final List<String> allTables = adapter.getAllTables(distributionGroupName);				

		logger.info("Tables to merge ({}): {}", destination.getIdentifier(), allTables);

		// Redistribute data
		for(final String tableNameString : allTables) {
			logger.info("Merging data of tuple store {}", tableNameString);
			
			final TupleStoreName tableName = new TupleStoreName(tableNameString);
			final TupleStoreName localName = tableName.cloneWithDifferntRegionId(destination.getRegionId());
			
			// Create table and init tuple store manager
			QueryHelper.getTupleStoreManager(registry, localName);

			final TupleRedistributor tupleRedistributor = new TupleRedistributor(registry, localName);
			tupleRedistributor.registerRegion(destination);

			for(final DistributionRegion childRegion : source) {
				mergeDataFromChildRegion(destination.getConveringBox(), localName, tupleRedistributor, childRegion);					
			}

			logger.info("Final statistics for merge ({}): {}", 
					localName, tupleRedistributor.getStatistics());
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
	private void mergeDataFromChildRegion(final Hyperrectangle queryBox, 
			final TupleStoreName tupleStoreName, final TupleRedistributor tupleRedistributor, 
			final DistributionRegion childRegion) throws StorageManagerException {

		try {
			final Consumer<Tuple> tupleConsumer = (t) -> {
				try {
					tupleRedistributor.redistributeTuple(t);
				} catch (StorageManagerException e) {
					logger.error("Got an exception while redistributing tuple", e);
				}
			};
			
			final RangeQueryExecutor rangeQueryExecutor = new RangeQueryExecutor(tupleStoreName, 
					queryBox, tupleConsumer, registry, ExecutionPolicy.ALL);

			rangeQueryExecutor.performDataRead();
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new StorageManagerException(e);
		} catch (Exception e) {
			throw new StorageManagerException(e);
		}
	}

}

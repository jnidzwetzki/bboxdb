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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.network.client.connection.BBoxDBConnection;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.network.routing.RoutingHopHelper;
import org.bboxdb.network.server.query.QueryHelper;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.ReadOnlyTupleStore;
import org.bboxdb.storage.tuplestore.manager.TupleStoreAquirer;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeQueryExecutor {
	
	public enum ExecutionPolicy {
		LOCAL_ONLY,
		NETWORK_ONLY,
		ALL;
	}

	/**
	 * The region to query
	 */
	private final TupleStoreName tupleStoreName;
	
	/**
	 * The range to query
	 */
	private final Hyperrectangle range;
	
	/**
	 * The consumer to process the tuples
	 */
	private final Consumer<Tuple> consumer;

	/**
	 * The execution policy
	 */
	private final ExecutionPolicy executionPolicy;
	
	/**
	 * The storage reference
	 */
	private final TupleStoreManagerRegistry registry;
	
	/**
	 * The performed reads on local data sources
	 */
	private int localReads = 0;

	/**
	 * The preformed reads on network data sources
	 */
	private int networkReads = 0;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RangeQueryExecutor.class);


	public RangeQueryExecutor(final TupleStoreName tupleStoreName, 
			final Hyperrectangle range, final Consumer<Tuple> consumer,
			final TupleStoreManagerRegistry registry,
			final ExecutionPolicy executionPolicy) {
				this.tupleStoreName = tupleStoreName;
				this.range = range;
				this.consumer = consumer;
				this.registry = registry;
				this.executionPolicy = executionPolicy;
	}
	
	/**
	 * Read the data
	 * @throws BBoxDBException 
	 */
	public void performDataRead() throws BBoxDBException, InterruptedException {
		final String distributionGroup = tupleStoreName.getDistributionGroup();
		
		final SpacePartitioner partitioner 
			= SpacePartitionerCache.getInstance().getSpacePartitionerForGroupName(distributionGroup);
		
		final List<DistributionRegion> regions = RoutingHopHelper.getRegionsForPredicate(
				partitioner.getRootNode(), range, DistributionRegionHelper.PREDICATE_REGIONS_FOR_READ);
				
		for(final DistributionRegion region : regions) {
			perfomReadOnRegion(region);
		}
		
		if(logger.isDebugEnabled()) {
			logger.debug("Completed range query with {} local reads and {} network reads on {} regions", 
				localReads, networkReads, regions.size());
		}
	}

	/**
	 * Perform the read on the given region, 
	 * a local read is preferred over a network read
	 * 
	 * @param region
	 * @throws StorageManagerException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 */
	private void perfomReadOnRegion(final DistributionRegion region) 
			throws InterruptedException, BBoxDBException {
		
		final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();

		try {
			if(region.getSystems().contains(localInstance)) {
				if(performLocalRead()) {
					readDataLocal(region);
					localReads++;
				}
			} else {
				if(performNetworkRead()) {
					readDataNetwork(region);	
					networkReads++;
				}
			}
			
		} catch (StorageManagerException | ZookeeperException e) {
			throw new BBoxDBException(e);
		}		
	}
	
	/** 
	 * Should be a network read performed
	 */
	private boolean performNetworkRead() {
		if(executionPolicy == ExecutionPolicy.ALL) {
			return true;
		}
		
		if(executionPolicy == ExecutionPolicy.NETWORK_ONLY) {
			return true;
		}
		
		return false;
	}

	/** 
	 * Should be a local read performed
	 */
	private boolean performLocalRead() {
		if(executionPolicy == ExecutionPolicy.ALL) {
			return true;
		}
		
		if(executionPolicy == ExecutionPolicy.LOCAL_ONLY) {
			return true;
		}
		
		return false;
	}

	/**
	 * Merge data by local data read
	 * 
	 * @param region
	 * @param tupleStoreName
	 * @param tupleRedistributor
	 * @param childRegion
	 * @throws StorageManagerException 
	 * @throws ZookeeperException 
	 * @throws InterruptedException 
	 * @throws BBoxDBException 
	 * @throws TupleStoreManagerRegistry 
	 */
	private void readDataLocal(final DistributionRegion region) 
			throws StorageManagerException, ZookeeperException, BBoxDBException, InterruptedException {

		final long regionId = region.getRegionId();
		final TupleStoreName childRegionName = tupleStoreName.cloneWithDifferntRegionId(regionId);
		final TupleStoreManager tupleStoreManager = QueryHelper.getTupleStoreManager(registry, childRegionName);
	
		try(final TupleStoreAquirer tupleStoreAquirer = new TupleStoreAquirer(tupleStoreManager)) {
			for(final ReadOnlyTupleStore storage : tupleStoreAquirer.getTupleStores()) {
				
				// When the region is complexly covered,
				// full table scan, otherwise index read
				if(range.isCovering(region.getConveringBox())) {
					for(final Tuple tuple : storage) {
						consumer.accept(tuple);
					}
				} else {
					final Iterator<Tuple> iter = storage.getAllTuplesInBoundingBox(range);
					while(iter.hasNext()) {
						final Tuple tuple = iter.next();
						consumer.accept(tuple);
					}
				}	
			}
		}
	}

	/**
	 * Merge the region by a network read
	 * 
	 * @param region
	 * @param tupleStoreName
	 * @param tupleRedistributor
	 * @param region
	 * @throws InterruptedException
	 * @throws StorageManagerException
	 * @throws Exception
	 */
	private void readDataNetwork(final DistributionRegion region) 
			throws InterruptedException, StorageManagerException {

		final List<BBoxDBInstance> systems = region.getSystems();
		assert(! systems.isEmpty()) : "Systems can not be empty";

		final BBoxDBInstance firstSystem = systems.get(0);

		final BBoxDBConnection connection = MembershipConnectionService.getInstance()
				.getConnectionForInstance(firstSystem);

		assert (connection != null) : "Connection can not be null: " + firstSystem.getStringValue();

		final Hyperrectangle bbox = region.getConveringBox();
		final String fullname = tupleStoreName.getFullname();
		final BBoxDBClient bboxDBClient = connection.getBboxDBClient();
		final TupleListFuture result = bboxDBClient.queryRectangle(fullname, bbox, new ArrayList<>());

		result.waitForCompletion();
		
		if(logger.isDebugEnabled()) {
			logger.debug("Remote range query took {} ms", result.getCompletionTime(TimeUnit.MILLISECONDS));
		}

		if(result.isFailed()) {
			throw new StorageManagerException("Exception while fetching tuples: " 
					+ result.getAllMessages());
		}

		for(final Tuple tuple : result) {
			consumer.accept(tuple);
		}
	}
}

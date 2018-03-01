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
package org.bboxdb.distribution.partitioner.regionsplit.tuplesink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleRedistributor {
	
	/**
	 * The tuple store name for data redistribution
	 */
	protected final TupleStoreName tupleStoreName;
	
	/**
	 * The list with the distribution regions
	 */
	protected final Map<DistributionRegion, List<AbstractTupleSink>> regionMap;
	
	/**
	 * The amount of total redistributed tuples
	 */
	protected long redistributedTuples;

	/**
	 * The storage registry
	 */
	protected final TupleStoreManagerRegistry tupleStoreManagerRegistry;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(TupleRedistributor.class);

	
	public TupleRedistributor(final TupleStoreManagerRegistry tupleStoreManagerRegistry, 
			final TupleStoreName tupleStoreName) {
		
		assert (tupleStoreManagerRegistry != null) : "Tuple store registry is null";
		assert (tupleStoreName != null) : "Tuple store name is null";
		assert (tupleStoreName.isValid()) : "Invalid tuple store name";

		this.tupleStoreManagerRegistry = tupleStoreManagerRegistry;
		this.tupleStoreName = tupleStoreName;
		this.regionMap = new HashMap<DistributionRegion, List<AbstractTupleSink>>();
		this.redistributedTuples = 0;
	}

	/**
	 * Register a new region for distribution
	 * @param distributionRegion
	 * @throws StorageManagerException 
	 */
	public void registerRegion(final DistributionRegion distributionRegion,
			final List<AbstractTupleSink> sinks) throws StorageManagerException {
		
		if(regionMap.containsKey(distributionRegion)) {
			throw new StorageManagerException("Region is already registered");
		}
		
		regionMap.put(distributionRegion, sinks);
	}
	
	/**
	 * Register a new region for distribution
	 * @param distributionRegion
	 * @throws StorageManagerException 
	 * @throws ZookeeperException 
	 */
	public void registerRegion(final DistributionRegion distributionRegion) 
			throws StorageManagerException {
		
		final ArrayList<AbstractTupleSink> sinks = new ArrayList<>();
	
		final Collection<BBoxDBInstance> instances = distributionRegion.getSystems();

		final MembershipConnectionService membershipConnectionService 	
			= MembershipConnectionService.getInstance();
				
		final BBoxDBInstance localInstance = ZookeeperClientFactory.getLocalInstanceName();
		
		for(final BBoxDBInstance instance : instances) {
			
			if(instance.socketAddressEquals(localInstance)) {
				
				final TupleStoreName localTableName = tupleStoreName.cloneWithDifferntRegionId(
						distributionRegion.getRegionId());
				
				final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
				final TupleStoreAdapter tupleStoreAdapter = new TupleStoreAdapter(zookeeperClient);
				final TupleStoreConfiguration config = readTuplestoreConfig(localTableName, tupleStoreAdapter);
				
				final TupleStoreManager storageManager = tupleStoreManagerRegistry.createTableIfNotExist(localTableName, config);
	
				final LocalTupleSink tupleSink = new LocalTupleSink(tupleStoreName, storageManager);
				sinks.add(tupleSink);
				logger.info("Redistributing data to local table {}", localTableName.getFullname());
			} else {
				final BBoxDBClient connection = membershipConnectionService.getConnectionForInstance(instance);
				final NetworkTupleSink tupleSink = new NetworkTupleSink(tupleStoreName, connection);
				sinks.add(tupleSink);
				logger.info("Redistributing data to remote system {}", instance.getInetSocketAddress());
			}
		}
		
		registerRegion(distributionRegion, sinks);
	}

	/**
	 * Read the given table configuration
	 * 
	 * @param localTableName
	 * @param tupleStoreAdapter
	 * @return
	 * @throws StorageManagerException 
	 */
	private TupleStoreConfiguration readTuplestoreConfig(final TupleStoreName localTableName,
			final TupleStoreAdapter tupleStoreAdapter) throws StorageManagerException  {
		
		try {
			return tupleStoreAdapter.readTuplestoreConfiguration(localTableName);
		} catch (ZookeeperException e) {
			throw new StorageManagerException(e);
		}
	}
	
	/**
	 * Redistribute a new tuple
	 * @param tuple
	 * @throws Exception 
	 */
	public void redistributeTuple(final Tuple tuple) throws StorageManagerException {
		
		boolean tupleRedistributed = false;
		
		redistributedTuples++;
		
		for(final DistributionRegion region : regionMap.keySet()) {
			if(belongsTupleToRegion(tuple, region)) {
				for(final AbstractTupleSink tupleSink : regionMap.get(region)) {
					tupleSink.sinkTuple(tuple);
					tupleRedistributed = true;
				}
			}
		}
		
		if(tupleRedistributed == false) {
			throw new StorageManagerException("Tuple " + tuple + " was not redistributed");
		}
	}

	/**
	 * Check if a tuple belongs to the given region
	 * 
	 * @param tuple
	 * @param region
	 * @return
	 */
	private boolean belongsTupleToRegion(final Tuple tuple, final DistributionRegion region) {
		// Tuple overlaps with region
		if(region.getConveringBox().overlaps(tuple.getBoundingBox())) {
			return true;
		}
		
		// Deleted tuples should always be redistributed
		if(TupleHelper.isDeletedTuple(tuple)) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Get the statistics for the redistribution
	 * @return
	 */
	public String getStatistics() {
		final StringBuilder sb = new StringBuilder();
		
		sb.append("Input tuples: " + redistributedTuples);
		
		float totalRedistributedTuples = 0;

		for(final DistributionRegion region : regionMap.keySet()) {
			if(regionMap.get(region).isEmpty()) {
				sb.append(", no systems for regionid " + region.getRegionId());
			} else {
				final long forwarededTuples = regionMap.get(region).get(0).getSinkedTuples();
				final float percent = ((float) forwarededTuples / (float) redistributedTuples * 100);
				sb.append(", forwared "+ forwarededTuples + " to regionid " + region.getRegionId());
				sb.append(String.format(" (%.2f %%)", percent));
				totalRedistributedTuples = totalRedistributedTuples + forwarededTuples;
			}
		}
		
		final float percent = ((float) totalRedistributedTuples / (float) redistributedTuples * 100);
		sb.append(" Total redistributed tuples: " + totalRedistributedTuples);
		sb.append(String.format(" (%.2f %%)", percent));

		return sb.toString();
	}
}

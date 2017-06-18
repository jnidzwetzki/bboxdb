/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.distribution.regionsplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.registry.StorageRegistry;
import org.bboxdb.storage.sstable.SSTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleRedistributor {
	
	/**
	 * The sstable name for data redistribution
	 */
	protected final SSTableName sstableName;
	
	/**
	 * The list with the distribution regions
	 */
	protected final Map<DistributionRegion, List<TupleSink>> regionMap;
	
	/**
	 * The amount of total redistributed tuples
	 */
	protected long redistributedTuples;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(TupleRedistributor.class);
	
	public TupleRedistributor(final SSTableName ssTableName) {
		this.sstableName = ssTableName;
		this.regionMap = new HashMap<DistributionRegion, List<TupleSink>>();
		this.redistributedTuples = 0;
		
		assert(ssTableName.isValid());
	}

	/**
	 * Register a new region for distribution
	 * @param distributionRegion
	 * @throws StorageManagerException 
	 */
	public void registerRegion(final DistributionRegion distributionRegion) 
			throws StorageManagerException {
		
		regionMap.put(distributionRegion, new ArrayList<TupleSink>());
		
		final Collection<DistributedInstance> instances = distributionRegion.getSystems();

		final MembershipConnectionService membershipConnectionService 	
			= MembershipConnectionService.getInstance();
		
		final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
		
		for(final DistributedInstance instance : instances) {
			
			if(instance.socketAddressEquals(zookeeperClient.getInstancename())) {
				
				final SSTableName localTableName = sstableName.cloneWithDifferntRegionId(
						distributionRegion.getRegionId());
				
				final SSTableManager storageManager = StorageRegistry.getInstance().getSSTableManager(localTableName);
				regionMap.get(distributionRegion).add(new LocalTupleSink(sstableName, storageManager));
			
				logger.info("Redistributing data to local table {}", localTableName.getFullname());
			} else {
				final BBoxDBClient connection = membershipConnectionService.getConnectionForInstance(instance);
				regionMap.get(distributionRegion).add(new NetworkTupleSink(sstableName, connection));
			
				logger.info("Redistributing data to remote system {}", instance.getInetSocketAddress());
			}
		}
	}
	
	/**
	 * Redistribute a new tuple
	 * @param tuple
	 * @throws Exception 
	 */
	public void redistributeTuple(final Tuple tuple) throws Exception {
		
		boolean tupleRedistributed = false;
		
		redistributedTuples++;
		
		for(final DistributionRegion region : regionMap.keySet()) {
			if(region.getConveringBox().overlaps(tuple.getBoundingBox())) {
				for(final TupleSink tupleSink : regionMap.get(region)) {
					tupleSink.sinkTuple(tuple);
					tupleRedistributed = true;
				}
			}
		}
		
		assert (tupleRedistributed == true) : "Tuple " + tuple + " was not redistribured";
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

abstract class TupleSink {
	
	/**
	 * The table name for the sink
	 */
	protected final String tablename;
	
	/**
	 * The amount of sinked tuples
	 */
	public long sinkedTuples;
	
	public TupleSink(final SSTableName tablename) {
		this.tablename = tablename.getFullname();
		this.sinkedTuples = 0;
	}
	
	/**
	 * Get the amount of sinked tuples
	 * @return
	 */
	public long getSinkedTuples() {
		return sinkedTuples;
	}
	
	public abstract void sinkTuple(final Tuple tuple) throws Exception;
}

class NetworkTupleSink extends TupleSink {
	
	/**
	 * The connection to spread data too
	 */
	protected final BBoxDBClient connection;
	
	public NetworkTupleSink(final SSTableName tablename, final BBoxDBClient connection) {
		super(tablename);
		this.connection = connection;
	}

	@Override
	public void sinkTuple(final Tuple tuple) {
		sinkedTuples++;
		connection.insertTuple(tablename, tuple);
	}
	
}

class LocalTupleSink extends TupleSink {

	/**
	 * The storage manager to store the tuple
	 */
	protected final SSTableManager storageManager;
	
	public LocalTupleSink(final SSTableName tablename, final SSTableManager storageManager) {
		super(tablename);
		this.storageManager = storageManager;
	}

	@Override
	public void sinkTuple(final Tuple tuple) throws Exception {
		sinkedTuples++;
		storageManager.put(tuple);
	}
}



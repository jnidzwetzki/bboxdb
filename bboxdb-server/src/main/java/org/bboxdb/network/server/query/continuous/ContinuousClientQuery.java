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
package org.bboxdb.network.server.query.continuous;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.network.server.query.ClientQuery;
import org.bboxdb.network.server.query.QueryHelper;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.ContinuousRangeQueryPlan;
import org.bboxdb.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousClientQuery implements ClientQuery {

	/**
	 * The bounding box of the query
	 */
	private final Hyperrectangle boundingBox;

	/**
	 * The client connection handler
	 */
	private final ClientConnectionHandler clientConnectionHandler;

	/**
	 * The package sequence of the query
	 */
	private final short querySequence;

	/**
	 * The total amount of send tuples
	 */
	private long totalSendTuples;
	
	/**
	 * The total amount of send tuples for this page
	 */
	private long tuplesInPage;
	
	/**
	 * Is the continuous query active
	 */
	private volatile boolean queryActive = true;

	/**
	 * The tuples for the given key
	 */
	private final BlockingQueue<MultiTuple> tupleQueue;
	
	/**
	 * The last query flush time
	 */
	private long lastFlushTime = System.currentTimeMillis();
	
	/**
	 * The number of tuples per page
	 */
	private final static long FLUSH_TIME_IN_MS = TimeUnit.SECONDS.toMillis(1);

	/**
	 * The tuple insert callback
	 */
	private final BiConsumer<TupleStoreName, Tuple> tupleInsertCallback;

	/**
	 * The tuple store manager
	 */
	private final List<TupleStoreManager> storageManager;
	
	/**
	 * The query plan
	 */
	private final ContinuousQueryPlan queryPlan;
	
	/**
	 * Allow the discarding of tuples when the queue is full
	 */
	private final boolean allowDiscardTuples;
	
	/**
	 * The query state
	 */
	private final ContinuousQueryExecutionState continuousQueryState = new ContinuousQueryExecutionState();

	/**
	 * The dead pill for the queue
	 */
	private final static MultiTuple RED_PILL = new MultiTuple(new ArrayList<>(), new ArrayList<>());
	
	/**
	 * Keep alive pill
	 */
	private final static MultiTuple KEEP_ALIVE_PILL = new MultiTuple(new ArrayList<>(), new ArrayList<>());
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousClientQuery.class);
	

	public ContinuousClientQuery(final ContinuousQueryPlan queryPlan,
			final ClientConnectionHandler clientConnectionHandler,
			final short querySequence) {

			this.queryPlan = queryPlan;
			this.boundingBox = queryPlan.getQueryRange();
			
			this.clientConnectionHandler = clientConnectionHandler;
			this.querySequence = querySequence;
			
			final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
			final int queueSize = configuration.getContinuousClientQueueSize();
			this.allowDiscardTuples = configuration.isAllowContinuousClientQueueDiscard();
			this.tupleQueue = new LinkedBlockingQueue<>(queueSize);

			this.totalSendTuples = 0;
			this.tuplesInPage = 0;
			this.storageManager = new ArrayList<>();

			// Add each tuple to our tuple queue
			if(queryPlan instanceof ContinuousRangeQueryPlan) {
				final ContinuousRangeQueryPlan qp = (ContinuousRangeQueryPlan) queryPlan;
				this.tupleInsertCallback = new ContinuousRangeQuery(this, qp);
			} else if(queryPlan instanceof ContinuousSpatialJoinQueryPlan) {
				final ContinuousSpatialJoinQueryPlan qp = (ContinuousSpatialJoinQueryPlan) queryPlan;
				this.tupleInsertCallback = new ContinuousSpatialJoinQuery(this, qp);
			} else { 
				this.tupleInsertCallback = null;
				logger.error("Unknown query type: " + queryPlan);
				queryActive = false;
				return;
			}

			try {
				init();
			} catch (BBoxDBException e) {
				logger.error("Got exception on init", e);
				queryActive = false;
			}
	}

	/**
	 * Init the query
	 * @param tupleStoreManagerRegistry
	 * @throws BBoxDBException
	 */
	private void init() throws BBoxDBException {

		try {
			
			final TupleStoreManagerRegistry storageRegistry
				= clientConnectionHandler.getStorageRegistry();
			
			final TupleStoreName requestTable = new TupleStoreName(queryPlan.getStreamTable());

			final String fullname = requestTable.getDistributionGroup();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(fullname);

			final DistributionRegionIdMapper regionIdMapper = spacePartitioner
					.getDistributionRegionIdMapper();

			final List<TupleStoreName> localTables
				= regionIdMapper.getLocalTablesForRegion(boundingBox, requestTable);
			
			logger.info("Starting new continuous client query (seq={}) on tables={}", querySequence, localTables);

			// Register insert new tuple callback
			for(final TupleStoreName tupleStoreName : localTables) {
				final TupleStoreManager tableStorageManager 
					= QueryHelper.getTupleStoreManager(storageRegistry, tupleStoreName);
				
				final boolean registerResult = tableStorageManager.registerInsertCallback(tupleInsertCallback);
				
				if(! registerResult) {
					logger.error("Unable to register query callback on {}", tupleStoreName.getFullname());
				}
				
				storageManager.add(tableStorageManager);
			}

			// Remove tuple store insert listener on connection close
			clientConnectionHandler.addConnectionClosedHandler((c) -> close());
		} catch (StorageManagerException | ZookeeperException e) {
			logger.error("Got an exception during query init", e);
			close();
		}
	}

	/**
	 * Queue the tuple for client processing
	 * @param tuple
	 */
	public void queueTupleForClientProcessing(final MultiTuple tuple) {
		
		if(allowDiscardTuples) {
			final boolean insertResult = tupleQueue.offer(tuple);
	
			if(! insertResult) {
				logger.error("Unable to add tuple to continuous query, queue is full (seq={} / size={})", 
						querySequence, tupleQueue.size());
			}
		} else {
			try {
				
				// Skip queuing when query is not longer active
				while(queryActive) {
					// Try to add and wait if queue is full
					final boolean submitted = tupleQueue.offer(tuple);
					
					if(submitted) {
						break;
					}
					
					Thread.sleep(100);
				}
			
			} catch (InterruptedException e) {
				logger.debug("Wait was interrupted", e);
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	@Override
	public void fetchAndSendNextTuples(final short packageSequence) throws IOException, PackageEncodeException {

		clientConnectionHandler.writeResultPackage(new MultipleTupleStartResponse(packageSequence));

		while(queryActive) {
			try {
				
				// Finish query _PAGE_ after at least FLUSH_TIME_IN_MS
				if(System.currentTimeMillis() >= lastFlushTime + FLUSH_TIME_IN_MS && tuplesInPage > 0) {
					
					logger.debug("Flushing page for continous query {}, old time {}, cur time {}, send tuples {}",
							packageSequence, lastFlushTime, System.currentTimeMillis(), totalSendTuples);
					
					clientConnectionHandler.writeResultPackage(new PageEndResponse(packageSequence));
					clientConnectionHandler.flushPendingCompressionPackages();
					lastFlushTime = System.currentTimeMillis();
					tuplesInPage = 0;
					return;
				}
				
				// Send next tuple or wait
				final MultiTuple tuple = tupleQueue.take();
				
				if(tuple == RED_PILL) {
					logger.info("Got the red pill from the queue, cancel query");
					clientConnectionHandler.writeResultPackage(new MultipleTupleEndResponse(packageSequence));
					clientConnectionHandler.flushPendingCompressionPackages();
					close();
					return;
				}
				
				// Test for query finish
				if(tuple == KEEP_ALIVE_PILL) {
					continue;
				}
				
				if(logger.isDebugEnabled()) {
					logger.debug("Adding tuple to query {}", packageSequence);
				}
				
				clientConnectionHandler.writeResultTuple(packageSequence, tuple, true);
				totalSendTuples++;
				tuplesInPage++;
				
			} catch (InterruptedException e) {
				logger.info("Thread was interrupted while waiting for new tuples");
				this.queryActive = false;
				clientConnectionHandler.writeResultPackage(new ErrorResponse(packageSequence));
				clientConnectionHandler.flushPendingCompressionPackages();
				return;
			}
		}

		// All tuples are send
		logger.info("Sending tuples for query {} is done", packageSequence);
		clientConnectionHandler.writeResultPackage(new MultipleTupleEndResponse(packageSequence));
		clientConnectionHandler.flushPendingCompressionPackages();	
	}
	

	@Override
	public void maintenanceCallback() {
		// Release waiting query processors and 
		// let them finish the query page if needed
		
		if(tupleQueue.isEmpty()) {
			tupleQueue.offer(KEEP_ALIVE_PILL);
		}
	}

	@Override
	public boolean isQueryDone() {
		return (! queryActive);
	}

	@Override
	public void close() {
		if(! queryActive) {
			return;
		}
		
		queryActive = false;
		
		logger.info("Closing query {} (send {} result tuples)", querySequence, totalSendTuples);

		for(final TupleStoreManager tableTupleStoreManager : storageManager) {
			final boolean removeResult = tableTupleStoreManager.removeInsertCallback(tupleInsertCallback);
			
			if(! removeResult) {
				logger.error("Unable to remove insert callback, got bad remove callback");
			}
		}
		
		if(storageManager.isEmpty()) {
			logger.error("Unable to remove insert callback, storage manager is NULL");
		}

		// Cancel next page request
		tupleQueue.clear();
		
		try {
			tupleQueue.put(RED_PILL);
		} catch (InterruptedException e) {
			logger.error("Interrupted while submitting red pill to queue", e);
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public long getTotalSendTuples() {
		return totalSendTuples;
	}
	
	/**
	 * Get the current query plan
	 * @return
	 */
	public ContinuousQueryPlan getQueryPlan() {
		return queryPlan;
	}
	
	/**
	 * Get the client connection handler
	 * @return
	 */
	public ClientConnectionHandler getClientConnectionHandler() {
		return clientConnectionHandler;
	}
	
	/**
	 * Cancel the query
	 */
	public void cancelQuery() {
		queryActive = false;
	}
	
	/**
	 * Get the continuous query state
	 * @return
	 */
	public ContinuousQueryExecutionState getContinuousQueryState() {
		return continuousQueryState;
	}
}

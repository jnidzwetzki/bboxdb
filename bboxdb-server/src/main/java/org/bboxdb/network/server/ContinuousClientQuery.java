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
package org.bboxdb.network.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor;
import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor.ExecutionPolicy;
import org.bboxdb.distribution.region.DistributionRegionIdMapper;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBConfiguration.ContinuousSpatialJoinFetchMode;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.ErrorResponse;
import org.bboxdb.network.packages.response.MultipleTupleEndResponse;
import org.bboxdb.network.packages.response.MultipleTupleStartResponse;
import org.bboxdb.network.packages.response.PageEndResponse;
import org.bboxdb.network.query.ContinuousRangeQueryPlan;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.network.query.entity.TupleAndBoundingBox;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.network.query.transformation.TupleTransformation;
import org.bboxdb.network.server.connection.ClientConnectionHandler;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
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
	 * The request table
	 */
	private final TupleStoreName requestTable;

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
	private boolean queryActive = true;

	/**
	 * The tuples for the given key
	 */
	private final BlockingQueue<MultiTuple> tupleQueue;
	
	/**
	 * The last query flush time
	 */
	private long lastFlushTime = System.currentTimeMillis();

	/**
	 * The maximal queue capacity
	 */
	private final static int MAX_QUEUE_CAPACITY = 1024;
	
	/**
	 * The number of tuples per page
	 */
	private final static long FLUSH_TIME_IN_MS = TimeUnit.SECONDS.toMillis(1);

	/**
	 * The tuple insert callback
	 */
	private final Consumer<Tuple> tupleInsertCallback;

	/**
	 * The tuple store manager
	 */
	private final List<TupleStoreManager> storageManager;
	
	/**
	 * The query plan
	 */
	private final ContinuousQueryPlan queryPlan;

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
			this.requestTable = new TupleStoreName(queryPlan.getStreamTable());
			
			this.clientConnectionHandler = clientConnectionHandler;
			this.querySequence = querySequence;
			this.tupleQueue = new ArrayBlockingQueue<>(MAX_QUEUE_CAPACITY);

			this.totalSendTuples = 0;
			this.tuplesInPage = 0;
			this.storageManager = new ArrayList<>();

			// Add each tuple to our tuple queue
			if(queryPlan instanceof ContinuousRangeQueryPlan) {
				final ContinuousRangeQueryPlan qp = (ContinuousRangeQueryPlan) queryPlan;
				this.tupleInsertCallback = getCallbackForRangeQuery(qp);
			} else if(queryPlan instanceof ContinuousSpatialJoinQueryPlan) {
				final ContinuousSpatialJoinQueryPlan qp = (ContinuousSpatialJoinQueryPlan) queryPlan;
				this.tupleInsertCallback = getCallbackForSpatialJoinQuery(qp);
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
	 * Get the callback for a spatial join query
	 * @param qp 
	 * @return
	 */
	private Consumer<Tuple> getCallbackForSpatialJoinQuery(final ContinuousSpatialJoinQueryPlan qp) {
		
		final List<TupleTransformation> streamTransformations = qp.getStreamTransformation(); 
		final Map<UserDefinedFilter, byte[]> streamFilters = getUserDefinedFilter(qp.getStreamFilters());
		final Map<UserDefinedFilter, byte[]> joinFilters = getUserDefinedFilter(qp.getAfterJoinFilter());
				
		return (streamTuple) -> {
			
			if(streamTuple instanceof DeletedTuple) {
				return;
			}
			
			final TupleAndBoundingBox transformedStreamTuple 
				= applyStreamTupleTransformations(streamTransformations, streamTuple);
			
			// Tuple was removed during transformation
			if(transformedStreamTuple == null) {
				return;
			}
			
			// Ignore stream elements outside of our query box
			if(! transformedStreamTuple.getBoundingBox().intersects(qp.getQueryRange())) {
				return;
			}
			
			// Perform stream UDFs
			final boolean udfMatches = doUserDefinedFilterMatch(streamTuple, streamFilters);
			
			if(! udfMatches) {
				return;
			}
			
			// Callback for the stored tuple
			final Consumer<Tuple> tupleConsumer = getStoredTupleReader(qp, joinFilters, streamTuple, transformedStreamTuple);
			
			try {
				final TupleStoreName tupleStoreName = new TupleStoreName(qp.getJoinTable());
				
				final TupleStoreManagerRegistry storageRegistry = clientConnectionHandler.getStorageRegistry();
				
				final ContinuousSpatialJoinFetchMode fetchMode = BBoxDBConfigurationManager.getConfiguration()
						.getContinuousSpatialJoinFetchModeENUM();
				
				// Handle non local data during spatial join
				ExecutionPolicy executionPolicy = ExecutionPolicy.LOCAL_ONLY;
				
				if(fetchMode == ContinuousSpatialJoinFetchMode.FETCH) {
					executionPolicy = ExecutionPolicy.ALL;
				} 
				
				final RangeQueryExecutor rangeQueryExecutor = new RangeQueryExecutor(tupleStoreName, 
						transformedStreamTuple.getBoundingBox(), 
						tupleConsumer, storageRegistry,
						executionPolicy);
				
				rangeQueryExecutor.performDataRead();
			} catch (BBoxDBException e) {
				logger.error("Got an exeeption while quering tuples", e);
				queryActive = false;
				return;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				queryActive = false;
				return;
			}	
		};
	}

	/**
	 * The callback handler for the range query of a continuous spatial join
	 * @param qp
	 * @param filters
	 * @param streamTuple
	 * @param transformedStreamTuple
	 * @return
	 */
	private Consumer<Tuple> getStoredTupleReader(final ContinuousSpatialJoinQueryPlan qp,
			final Map<UserDefinedFilter, byte[]> filters, final Tuple streamTuple,
			final TupleAndBoundingBox transformedStreamTuple) {
		
		return (storedTuple) -> {
			final List<TupleTransformation> tableTransformations 
				= qp.getTableTransformation(); 
			
			final TupleAndBoundingBox transformedStoredTuple 
				= applyStreamTupleTransformations(tableTransformations, storedTuple);
			
			if(transformedStoredTuple == null) {
				logger.error("Transformed tuple is null, please check filter");
				return;
			}
			
			final boolean intersection = transformedStreamTuple.getBoundingBox()
					.intersects(transformedStoredTuple.getBoundingBox());
			
			// Is the tuple important for the query?
			if(intersection) {
			
				// Perform expensive UDF
				final boolean udfMatches = doUserDefinedFilterMatch(streamTuple, 
						storedTuple, filters);

				if(udfMatches == true) {
					final MultiTuple joinedTuple = buildJoinedTuple(qp, streamTuple, storedTuple);
					queueTupleForClientProcessing(joinedTuple);
				}
				
			}
		};
	}

	/**
	 * Build a joined tuple 
	 * @param qp
	 * @param streamTuple
	 * @param storedTuple
	 * @return
	 */
	private MultiTuple buildJoinedTuple(final ContinuousSpatialJoinQueryPlan qp, 
			final Tuple streamTuple, final Tuple storedTuple) {
		
		return new MultiTuple(
				Arrays.asList(streamTuple, storedTuple), 
				Arrays.asList(qp.getStreamTable(), qp.getJoinTable()));
	}

	
	/**
	 * Perform the user defined filters on the given stream and stored tuple
	 * @param streamTuple
	 * @param storedTuple
	 * @param filters
	 * @return
	 */
	private boolean doUserDefinedFilterMatch(final Tuple streamTuple,
			final Map<UserDefinedFilter, byte[]> filters) {
		
		boolean matches = true;
		
		for(final Entry<UserDefinedFilter, byte[]> entry : filters.entrySet()) {
			
			final UserDefinedFilter operator = entry.getKey();
			final byte[] value = entry.getValue();
			
			final boolean result 
				= operator.filterTuple(streamTuple, value);
			
			if(! result) {
				matches = false;
			}
		}
		return matches;
	}
	
	/**
	 * Perform the user defined filters on the given stream and stored tuple
	 * @param streamTuple
	 * @param storedTuple
	 * @param filters
	 * @return
	 */
	private boolean doUserDefinedFilterMatch(final Tuple streamTuple, final Tuple storedTuple,
			final Map<UserDefinedFilter, byte[]> filters) {
		
		boolean matches = true;
		
		for(final Entry<UserDefinedFilter, byte[]> entry : filters.entrySet()) {
			
			final UserDefinedFilter operator = entry.getKey();
			final byte[] value = entry.getValue();
			
			final boolean result 
				= operator.filterJoinCandidate(streamTuple, storedTuple, value);
			
			if(! result) {
				matches = false;
			}
		}
		return matches;
	}

	/**
	 * Get the user defined operators
	 * @param filters
	 * @return 
	 */
	private Map<UserDefinedFilter, byte[]> getUserDefinedFilter(
			final List<UserDefinedFilterDefinition> filters) {
		
		final Map<UserDefinedFilter, byte[]> operators = new HashMap<>();
		
		for(final UserDefinedFilterDefinition filter : filters) {
			try {
				final Class<?> filterClass = Class.forName(filter.getUserDefinedFilterClass());
				final UserDefinedFilter operator = 
						(UserDefinedFilter) filterClass.newInstance();
				operators.put(operator, filter.getUserDefinedFilterValue().getBytes());
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				throw new IllegalArgumentException("Unable to find user defined filter class", e);
			}
		}
		
		return operators;
	}

	/**
	 * Get the consumer for the range query
	 * @param qp 
	 * @return
	 */
	private Consumer<Tuple> getCallbackForRangeQuery(final ContinuousRangeQueryPlan qp) {
		
		final ContinuousRangeQueryPlan rangeQueryPlan = (ContinuousRangeQueryPlan) queryPlan;
		
		final Map<UserDefinedFilter, byte[]> streamFilters = getUserDefinedFilter(qp.getStreamFilters());
		
		return (streamTuple) -> {
			
			if(streamTuple instanceof DeletedTuple) {
				return;
			}
			
			final List<TupleTransformation> transformations = rangeQueryPlan.getStreamTransformation(); 
			final TupleAndBoundingBox tuple = applyStreamTupleTransformations(transformations, streamTuple);
			
			// Tuple was removed during transformation
			if(tuple == null) {
				return;
			}
			
			// Perform stream UDFs
			final boolean udfMatches = doUserDefinedFilterMatch(streamTuple, streamFilters);
			
			if(! udfMatches) {
				return;
			}
			
			// Is the tuple important for the query?
			if(tuple.getBoundingBox().intersects(rangeQueryPlan.getCompareRectangle())) {
				if(rangeQueryPlan.isReportPositive()) {
					final MultiTuple joinedTuple = new MultiTuple(streamTuple, requestTable.getFullname());
					queueTupleForClientProcessing(joinedTuple);
				}
			} else {
				if(! rangeQueryPlan.isReportPositive()) {
					final MultiTuple joinedTuple = new MultiTuple(streamTuple, requestTable.getFullname());
					queueTupleForClientProcessing(joinedTuple);
				}
			}

		};
	}

	/**
	 * Apply the stream transformations
	 * @param constQueryPlan
	 * @param inputTuple
	 * @return
	 */
	private TupleAndBoundingBox applyStreamTupleTransformations(final List<TupleTransformation> transformations,
			final Tuple inputTuple) {
				
		TupleAndBoundingBox tuple = new TupleAndBoundingBox(inputTuple, inputTuple.getBoundingBox());
		for(final TupleTransformation transformation : transformations) {
			tuple = transformation.apply(tuple);
			
			if(tuple == null) {
				break;
			}
		}
		
		return tuple;
	}

	/**
	 * Queue the tuple for client processing
	 * @param t
	 */
	private void queueTupleForClientProcessing(final MultiTuple t) {
		final boolean insertResult = tupleQueue.offer(t);

		if(! insertResult) {
			logger.error("Unable to add tuple to continuous query, queue is full (seq={})", querySequence);
		}
	}

	/**
	 * Init the query
	 * @param tupleStoreManagerRegistry
	 * @throws BBoxDBException
	 */
	private void init() throws BBoxDBException {

		try {
			logger.info("Starting new continuous client query (seq={})", querySequence);
			
			final TupleStoreManagerRegistry storageRegistry
				= clientConnectionHandler.getStorageRegistry();

			final String fullname = requestTable.getDistributionGroup();
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(fullname);

			final DistributionRegionIdMapper regionIdMapper = spacePartitioner
					.getDistributionRegionIdMapper();

			final List<TupleStoreName> localTables
				= regionIdMapper.getLocalTablesForRegion(boundingBox, requestTable);

			// Register insert new tuple callback
			for(final TupleStoreName tupleStoreName : localTables) {
				final TupleStoreManager tableStorageManager 
					= QueryHelper.getTupleStoreManager(storageRegistry, tupleStoreName);
				
				tableStorageManager.registerInsertCallback(tupleInsertCallback);
				
				storageManager.add(tableStorageManager);
			}

			// Remove tuple store insert listener on connection close
			clientConnectionHandler.addConnectionClosedHandler((c) -> close());
		} catch (StorageManagerException | ZookeeperException e) {
			logger.error("Got an exception during query init", e);
			close();
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

		queryActive = false;
		
		// Cancel next page request
		tupleQueue.offer(RED_PILL);
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
}

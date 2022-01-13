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
package org.bboxdb.network.server.query.continuous;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor;
import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor.ExecutionPolicy;
import org.bboxdb.misc.BBoxDBConfiguration.ContinuousSpatialJoinFetchMode;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.network.entity.TupleAndBoundingBox;
import org.bboxdb.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.query.filter.UserDefinedFilter;
import org.bboxdb.query.transformation.TupleTransformation;
import org.bboxdb.storage.entity.InvalidationTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousSpatialJoinQuery extends AbstractContinuousQuery<ContinuousSpatialJoinQueryPlan> {
	
	/**
	 * The stream filters
	 */
	private final Map<UserDefinedFilter, byte[]> streamFilters;
	
	/**
	 * The join filters
	 */
	private final Map<UserDefinedFilter, byte[]> joinFilters;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousSpatialJoinQuery.class);

	public ContinuousSpatialJoinQuery(final ContinuousClientQuery continuousClientQuery, 
			final ContinuousSpatialJoinQueryPlan queryPlan) {
		
		super(continuousClientQuery, queryPlan);
		
		this.streamFilters = ContinuousQueryHelper.getUserDefinedFilter(queryPlan.getStreamFilters());
		this.joinFilters = ContinuousQueryHelper.getUserDefinedFilter(queryPlan.getAfterJoinFilter());
	}
	
	@Override
	public void accept(final TupleStoreName tupleStoreName, final Tuple streamTuple) {
		
		final boolean processFurtherActions = processSpecialTuples(tupleStoreName, streamTuple);
		
		if(! processFurtherActions) {
			return;
		}
		
		final TupleStoreName joinTableName = new TupleStoreName(queryPlan.getJoinTable());

		final List<TupleTransformation> streamTransformations = queryPlan.getStreamTransformation(); 

		final TupleAndBoundingBox transformedStreamTuple 
			= ContinuousQueryHelper.applyStreamTupleTransformations(streamTransformations, streamTuple);
		
		// Tuple was removed during transformation
		if(transformedStreamTuple == null) {
			handleNonMatch(streamTuple);
			return;
		}
		
		// Ignore stream elements outside of our query box
		if(! transformedStreamTuple.getBoundingBox().intersects(queryPlan.getQueryRange())) {
			handleNonMatch(streamTuple);
			return;
		}
		
		// Perform stream UDFs
		final boolean udfMatches = ContinuousQueryHelper.doUserDefinedFilterMatch(
				streamTuple, streamFilters);
		
		if(! udfMatches) {
			handleNonMatch(streamTuple);
			return;
		}
		
		// Callback for the stored tuple
		final Consumer<Tuple> tupleConsumer = getStoredTupleReader(queryPlan, joinFilters, streamTuple, transformedStreamTuple);
		
		try {
			final TupleStoreManagerRegistry storageRegistry = continuousClientQuery
					.getClientConnectionHandler().getStorageRegistry();
			
			final ContinuousSpatialJoinFetchMode fetchMode = BBoxDBConfigurationManager.getConfiguration()
					.getContinuousSpatialJoinFetchModeENUM();
			
			// Handle non local data during spatial join
			ExecutionPolicy executionPolicy = ExecutionPolicy.LOCAL_ONLY;
			
			if(fetchMode == ContinuousSpatialJoinFetchMode.FETCH) {
				executionPolicy = ExecutionPolicy.ALL;
			} 
			
			final RangeQueryExecutor rangeQueryExecutor = new RangeQueryExecutor(joinTableName, 
					transformedStreamTuple.getBoundingBox(), 
					tupleConsumer, storageRegistry,
					executionPolicy);
			
			rangeQueryExecutor.performDataRead();
			
			handleJoinMatchFinal(streamTuple);
		} catch (BBoxDBException e) {
			logger.error("Got an exeeption while quering tuples", e);
			continuousClientQuery.cancelQuery();
			return;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			continuousClientQuery.cancelQuery();
			return;
		}	
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
				= ContinuousQueryHelper.applyStreamTupleTransformations(
						tableTransformations, storedTuple);
			
			if(transformedStoredTuple == null) {
				logger.error("Transformed tuple is null, please check filter");
				return;
			}
			
			final boolean intersection = transformedStreamTuple.getBoundingBox()
					.intersects(transformedStoredTuple.getBoundingBox());
			
			// Is the tuple important for the query?
			if(intersection) {
			
				// Perform expensive UDF
				final boolean udfMatches = ContinuousQueryHelper.doUserDefinedFilterMatch(
						streamTuple, storedTuple, filters);

				if(udfMatches == true) {
					final MultiTuple joinedTuple = new MultiTuple(
							Arrays.asList(streamTuple, storedTuple), 
							Arrays.asList(qp.getStreamTable(), qp.getJoinTable()));
					
					handleJoinMatch(streamTuple, storedTuple);
					continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
				}
				
			}
		};
	}

	/**
	 * Handle the non match of the stream tuple
	 * @param streamTuple
	 */
	protected void handleNonMatch(final Tuple streamTuple) {
		if(! queryPlan.isReceiveInvalidations()) {
			return;
		}
		
		generateInvalidationTuplesForStreamKey(streamTuple);
	}
	
	/**
	 * Handle the non match of the stream tuple
	 * @param streamTuple
	 */
	protected void handleJoinMatch(final Tuple streamTuple, final Tuple joinPartner) {
		if(! queryPlan.isReceiveInvalidations()) {
			return;
		}
		
		final ContinuousQueryExecutionState continuousQueryState = continuousClientQuery.getContinuousQueryState();

		continuousQueryState.addJoinCandidateForCurrentKey(joinPartner.getKey());
	}

	/**
	 * Handle the non match of the stream tuple
	 * @param streamTuple
	 */
	protected void handleJoinMatchFinal(final Tuple streamTuple) {
		if(! queryPlan.isReceiveInvalidations()) {
			return;
		}
		
		generateInvalidationTuplesForStreamKey(streamTuple);
	}
	
	@Override
	protected void handleInvalidationTuple(final Tuple streamTuple) {	
		final ContinuousQueryExecutionState continuousQueryState = continuousClientQuery.getContinuousQueryState();
		final String streamKey = streamTuple.getKey();

		continuousClientQuery.getContinuousQueryState().clearJoinPartnerState();
		
		if(continuousQueryState.wasStreamKeyContainedInLastJoinQuery(streamKey)) {
			generateInvalidationTuplesForStreamKey(streamTuple);
			continuousQueryState.removeStreamKeyFromJoinState(streamKey);
		}
	}

	/**
	 * Generate the invalidation tuples for the current stream key
	 * @param streamTuple
	 * @param continuousQueryState
	 * @param streamKey
	 */
	private void generateInvalidationTuplesForStreamKey(final Tuple streamTuple) {
		
		final ContinuousQueryExecutionState continuousQueryState = continuousClientQuery.getContinuousQueryState();
		final String streamKey = streamTuple.getKey();
		
		final List<String> tables = Arrays.asList(queryPlan.getStreamTable(), queryPlan.getJoinTable());
		
		// Invalidate join query results
		final Set<String> joinPartners = continuousQueryState.commitStateAndGetMissingJoinpartners(streamKey);
		
		for(final String joinPartner : joinPartners) {
			final long versionTimestamp = streamTuple.getVersionTimestamp();
			final InvalidationTuple tuple = new InvalidationTuple(streamKey, versionTimestamp);
			
			final Tuple joinPartnerTuple = new Tuple(joinPartner, Hyperrectangle.FULL_SPACE, "".getBytes(Const.DEFAULT_CHARSET));
			
			final MultiTuple joinedTuple = new MultiTuple(Arrays.asList(tuple, joinPartnerTuple), tables);
			continuousClientQuery.queueTupleForClientProcessing(joinedTuple);	
		}
	}

}

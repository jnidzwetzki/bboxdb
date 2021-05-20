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

import java.util.List;
import java.util.Map;

import org.bboxdb.network.entity.TupleAndBoundingBox;
import org.bboxdb.network.query.ContinuousRangeQueryPlan;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.query.transformation.TupleTransformation;
import org.bboxdb.storage.entity.InvalidationTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousRangeQuery extends AbstractContinuousQuery<ContinuousRangeQueryPlan> {

	/**
	 * The stream filters
	 */
	private final Map<UserDefinedFilter, byte[]> streamFilters;

	/**
	 * The state of the query
	 */
	private final ContinuousQueryExecutionState continuousQueryState;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousRangeQuery.class);
	
	public ContinuousRangeQuery(final ContinuousClientQuery continuousClientQuery, 
			final ContinuousRangeQueryPlan queryPlan) {
		
		super(continuousClientQuery, queryPlan);
		
		this.streamFilters = ContinuousQueryHelper.getUserDefinedFilter(queryPlan.getStreamFilters());
		continuousQueryState = continuousClientQuery.getContinuousQueryState();
	}

	@Override
	public void accept(final TupleStoreName tupleStoreName, final Tuple streamTuple) {
		
		final boolean processFurtherActions = processSpecialTuples(tupleStoreName, streamTuple);
		
		if(! processFurtherActions) {
			return;
		}
		
		final List<TupleTransformation> transformations = queryPlan.getStreamTransformation(); 
		final TupleAndBoundingBox tuple = ContinuousQueryHelper.applyStreamTupleTransformations(
				transformations, streamTuple);
		
		// Tuple was removed during transformation
		if(tuple == null) {
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
		
		// Is the tuple important for the query?
		if(tuple.getBoundingBox().intersects(queryPlan.getCompareRectangle())) {
			if(queryPlan.isReportPositive()) {
				final MultiTuple joinedTuple = new MultiTuple(streamTuple, queryPlan.getStreamTable());
				handleMatch(streamTuple);
				continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
			} else {
				handleNonMatch(streamTuple);
			}
		} else {
			if(! queryPlan.isReportPositive()) {
				final MultiTuple joinedTuple = new MultiTuple(streamTuple, queryPlan.getStreamTable());
				handleMatch(streamTuple);
				continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
			} else {
				handleNonMatch(streamTuple);
			}
		}
	}
	
	/**
	 * Handle the non match of the stream tuple
	 * @param streamTuple
	 */
	protected void handleNonMatch(final Tuple streamTuple) {
		if(! queryPlan.isReceiveInvalidations()) {
			return;
		}
		
		final String streamKey = streamTuple.getKey();
		
		if(continuousQueryState.wasStreamKeyContainedInLastQuery(streamKey)) {
			logger.debug("Key {} was contained in last execution, sending invalidation tuple", streamKey);
			generateInvalidationTuple(streamTuple, continuousQueryState, streamKey);
		}
	}
	
	/**
	 * Handle the match of the stream tuple
	 * @param streamTuple
	 */
	protected void handleMatch(final Tuple streamTuple) {
		if(! queryPlan.isReceiveInvalidations()) {
			return;
		}
		
		continuousQueryState.addStreamKeyToState(streamTuple.getKey());
	}

	@Override
	protected void handleInvalidationTuple(final Tuple streamTuple) {
		final ContinuousQueryExecutionState continuousQueryState = continuousClientQuery.getContinuousQueryState();
		final String streamKey = streamTuple.getKey();
		
		// Invalidate range query results
		if(continuousQueryState.wasStreamKeyContainedInLastQuery(streamKey)) {
			generateInvalidationTuple(streamTuple, continuousQueryState, streamKey);
		}
	}

	/**
	 * Generate an invalidation tuple for the stream key
	 * @param streamTuple
	 * @param continuousQueryState
	 * @param streamKey
	 */
	private void generateInvalidationTuple(final Tuple streamTuple,
			final ContinuousQueryExecutionState continuousQueryState, final String streamKey) {
		
		continuousQueryState.removeStreamKeyFromState(streamKey);
		
		final long versionTimestamp = streamTuple.getVersionTimestamp();
		final InvalidationTuple tuple = new InvalidationTuple(streamKey, versionTimestamp);
		final MultiTuple joinedTuple = new MultiTuple(tuple, queryPlan.getStreamTable());
		continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
	}

}

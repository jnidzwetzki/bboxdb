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
import java.util.function.BiConsumer;

import org.bboxdb.network.query.ContinuousRangeQueryPlan;
import org.bboxdb.network.query.entity.TupleAndBoundingBox;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.query.transformation.TupleTransformation;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.entity.WatermarkTuple;

public class ContinuousRangeQuery implements BiConsumer<TupleStoreName, Tuple> {

	/**
	 * The associated client query
	 */
	private final ContinuousClientQuery continuousClientQuery;
	
	/**
	 * The query plan of the query
	 */
	private final ContinuousRangeQueryPlan queryPlan;

	/**
	 * The stream filters
	 */
	private final Map<UserDefinedFilter, byte[]> streamFilters;
		
	public ContinuousRangeQuery(final ContinuousClientQuery continuousClientQuery, 
			final ContinuousRangeQueryPlan queryPlan) {
		this.continuousClientQuery = continuousClientQuery;
		this.queryPlan = queryPlan;
		this.streamFilters = ContinuousQueryHelper.getUserDefinedFilter(queryPlan.getStreamFilters());
	}

	@Override
	public void accept(final TupleStoreName tupleStoreName, final Tuple streamTuple) {
		if(streamTuple instanceof DeletedTuple) {
			return;
		}
		
		if(streamTuple instanceof WatermarkTuple) {
			
			if(queryPlan.isReceiveWatermarks()) {
				final MultiTuple joinedTuple = ContinuousQueryHelper.getWatermarkTupleForLocalInstance(tupleStoreName, streamTuple);
				continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
			}
			
			return;
		}
		
		final List<TupleTransformation> transformations = queryPlan.getStreamTransformation(); 
		final TupleAndBoundingBox tuple = ContinuousQueryHelper.applyStreamTupleTransformations(
				transformations, streamTuple);
		
		// Tuple was removed during transformation
		if(tuple == null) {
			return;
		}
		
		// Perform stream UDFs
		final boolean udfMatches = ContinuousQueryHelper.doUserDefinedFilterMatch(
				streamTuple, streamFilters);
		
		if(! udfMatches) {
			return;
		}
		
		// Is the tuple important for the query?
		if(tuple.getBoundingBox().intersects(queryPlan.getCompareRectangle())) {
			if(queryPlan.isReportPositive()) {
				final MultiTuple joinedTuple = new MultiTuple(streamTuple, queryPlan.getStreamTable());
				continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
			}
		} else {
			if(! queryPlan.isReportPositive()) {
				final MultiTuple joinedTuple = new MultiTuple(streamTuple, queryPlan.getStreamTable());
				continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
			}
		}
	}

}

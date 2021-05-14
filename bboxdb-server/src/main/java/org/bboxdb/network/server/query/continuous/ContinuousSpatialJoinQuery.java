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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor;
import org.bboxdb.distribution.partitioner.regionsplit.RangeQueryExecutor.ExecutionPolicy;
import org.bboxdb.misc.BBoxDBConfiguration.ContinuousSpatialJoinFetchMode;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.network.query.entity.TupleAndBoundingBox;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.network.query.transformation.TupleTransformation;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.entity.WatermarkTuple;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManagerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousSpatialJoinQuery implements BiConsumer<TupleStoreName, Tuple> {
	
	/**
	 * The associated client query
	 */
	private final ContinuousClientQuery continuousClientQuery;
	
	/**
	 * The query plan of the query
	 */
	private final ContinuousSpatialJoinQueryPlan queryPlan;

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
		
		this.continuousClientQuery = continuousClientQuery;
		this.queryPlan = queryPlan;
		this.streamFilters = ContinuousQueryHelper.getUserDefinedFilter(queryPlan.getStreamFilters());
		this.joinFilters = ContinuousQueryHelper.getUserDefinedFilter(queryPlan.getAfterJoinFilter());
	}
	
	@Override
	public void accept(final TupleStoreName tupleStoreName, final Tuple streamTuple) {
		if(streamTuple instanceof DeletedTuple) {
			return;
		}
		
		if(streamTuple instanceof WatermarkTuple) {
			
			if(queryPlan.isReceiveWatermarks()) {
				final MultiTuple multiTuple = ContinuousQueryHelper.getWatermarkTupleForLocalInstance(tupleStoreName, streamTuple);
				continuousClientQuery.queueTupleForClientProcessing(multiTuple);
			}
			
			return;
		}
		
		final TupleStoreName joinTableName = new TupleStoreName(queryPlan.getJoinTable());

		final List<TupleTransformation> streamTransformations = queryPlan.getStreamTransformation(); 

		final TupleAndBoundingBox transformedStreamTuple 
			= ContinuousQueryHelper.applyStreamTupleTransformations(streamTransformations, streamTuple);
		
		// Tuple was removed during transformation
		if(transformedStreamTuple == null) {
			return;
		}
		
		// Ignore stream elements outside of our query box
		if(! transformedStreamTuple.getBoundingBox().intersects(queryPlan.getQueryRange())) {
			return;
		}
		
		// Perform stream UDFs
		final boolean udfMatches = ContinuousQueryHelper.doUserDefinedFilterMatch(
				streamTuple, streamFilters);
		
		if(! udfMatches) {
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
					
					continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
				}
				
			}
		};
	}

}

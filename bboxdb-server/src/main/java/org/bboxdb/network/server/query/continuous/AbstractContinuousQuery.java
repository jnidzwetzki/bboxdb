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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.Const;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.IdleQueryStateRemovedTuple;
import org.bboxdb.storage.entity.InvalidationTuple;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.entity.WatermarkTuple;

public abstract class AbstractContinuousQuery<T extends ContinuousQueryPlan> implements BiConsumer<TupleStoreName, Tuple> {
	
	/**
	 * The associated client query
	 */
	protected final ContinuousClientQuery continuousClientQuery;
	
	/**
	 * The query plan of the query
	 */
	protected final T queryPlan;
	
	/**
	 * The current watermark generation
	 */
	protected long watermarkGeneration;

	public AbstractContinuousQuery(final ContinuousClientQuery continuousClientQuery, final T queryPlan) {
		this.continuousClientQuery = continuousClientQuery;
		this.queryPlan = queryPlan;
		this.watermarkGeneration = 0;
	}
	
	/**
	 * Process special tuples
	 * @param tupleStoreName
	 * @param streamTuple
	 * @return
	 */
	protected boolean processSpecialTuples(final TupleStoreName tupleStoreName, final Tuple streamTuple) {
		
		if(streamTuple instanceof DeletedTuple) {
			return false;
		}
		
		if(streamTuple instanceof WatermarkTuple) {
			
			if(queryPlan.isReceiveWatermarks()) {
				final MultiTuple joinedTuple = ContinuousQueryHelper.getWatermarkTuple(tupleStoreName, streamTuple);
				continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
			}
			
			// Perform a removal of idle state entries
			final long invalidationGenerations = queryPlan.getInvalidateStateAfterWatermarks();
			
			if(invalidationGenerations > 0) {
				final ContinuousQueryExecutionState state = continuousClientQuery.getContinuousQueryState();
				final Optional<IdleQueryStateResult> idleElementsOptional 
					= state.invalidateIdleEntries(watermarkGeneration, invalidationGenerations);
				
				if(idleElementsOptional.isPresent()) {
					generateQueryStateRemovalTuples(idleElementsOptional.get());
				}
				
				watermarkGeneration++;
				state.setCurrentWatermarkGeneration(watermarkGeneration);
			}
			
			return false;
		}
		
		// Stream tuple is not longer contained in current region
		if(streamTuple instanceof InvalidationTuple) {
			
			if(! queryPlan.isReceiveInvalidations()) {
				return false;
			}
			
			handleInvalidationTuple(streamTuple);
					
			return false;
		}
		
		return true;
	}

	/**
	 * Generate IdleQueryStateRemovedTuple for all removed elements from the state
	 * @param idleElements
	 */
	protected void generateQueryStateRemovalTuples(final IdleQueryStateResult idleElements) {
		
		// Range query state
		for(final String key : idleElements.getRemovedStreamKeys()) {
			final Tuple tuple = new IdleQueryStateRemovedTuple(key);
			final MultiTuple joinedTuple = new MultiTuple(tuple, queryPlan.getStreamTable());
			continuousClientQuery.queueTupleForClientProcessing(joinedTuple);
		}
		
		if(queryPlan instanceof ContinuousSpatialJoinQueryPlan) {
			
			final ContinuousSpatialJoinQueryPlan continuousSpatialJoinQueryPlan = (ContinuousSpatialJoinQueryPlan) queryPlan;
			
			// Join query state
			for(final Map.Entry<String, Set<String>> entry : idleElements.getRemovedJoinPartners().entrySet()) {
				
				final List<String> tables = Arrays.asList(queryPlan.getStreamTable(), continuousSpatialJoinQueryPlan.getJoinTable());
							
				for(final String joinPartner : entry.getValue()) {
					final Tuple tuple = new IdleQueryStateRemovedTuple(entry.getKey());
					final Tuple joinPartnerTuple = new Tuple(joinPartner, Hyperrectangle.FULL_SPACE, "".getBytes(Const.DEFAULT_CHARSET));
					
					final MultiTuple joinedTuple = new MultiTuple(Arrays.asList(tuple, joinPartnerTuple), tables);
					continuousClientQuery.queueTupleForClientProcessing(joinedTuple);	
				}
				
			}
		}
		

		
	}

	/**
	 * Handle the global invalidation tuple
	 * @param tuple
	 */
	protected abstract void handleInvalidationTuple(final Tuple tuple);
}

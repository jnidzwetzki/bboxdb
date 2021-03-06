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

import java.util.function.BiConsumer;

import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.storage.entity.DeletedTuple;
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

	public AbstractContinuousQuery(final ContinuousClientQuery continuousClientQuery, final T queryPlan) {
		this.continuousClientQuery = continuousClientQuery;
		this.queryPlan = queryPlan;
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
	 * Handle the global invalidation tuple
	 * @param tuple
	 */
	protected abstract void handleInvalidationTuple(final Tuple tuple);
}

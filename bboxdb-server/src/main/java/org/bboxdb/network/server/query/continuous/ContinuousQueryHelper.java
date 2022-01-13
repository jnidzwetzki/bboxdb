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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bboxdb.misc.Const;
import org.bboxdb.network.entity.TupleAndBoundingBox;
import org.bboxdb.query.filter.UserDefinedFilter;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.transformation.TupleTransformation;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.entity.WatermarkTuple;
import org.bboxdb.storage.sstable.SSTableConst;

public class ContinuousQueryHelper {
	
	/**
	 * Get the watermark tuple for the current instance
	 * @param streamTuple
	 * @return
	 */
	public static MultiTuple getWatermarkTuple(final TupleStoreName tupleStorename, 
			final Tuple streamTuple) {
		
		final String key = SSTableConst.WATERMARK_KEY + "_" + tupleStorename.getFullname();
		final WatermarkTuple watermarkTuple = new WatermarkTuple(key, streamTuple.getVersionTimestamp());
		return new MultiTuple(watermarkTuple, tupleStorename.getTablename());
	}
	
	/**
	 * Apply the stream transformations
	 * @param constQueryPlan
	 * @param inputTuple
	 * @return
	 */
	public static TupleAndBoundingBox applyStreamTupleTransformations(
			final List<TupleTransformation> transformations,
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
	 * Get the user defined operators
	 * @param filters
	 * @return 
	 */
	public static Map<UserDefinedFilter, byte[]> getUserDefinedFilter(
			final List<UserDefinedFilterDefinition> filters) {
		
		final Map<UserDefinedFilter, byte[]> operators = new HashMap<>();
		
		for(final UserDefinedFilterDefinition filter : filters) {
			try {
				final Class<?> filterClass = Class.forName(filter.getUserDefinedFilterClass());
				final UserDefinedFilter operator = 
						(UserDefinedFilter) filterClass.newInstance();
				operators.put(operator, filter.getUserDefinedFilterValue().getBytes(Const.DEFAULT_CHARSET));
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				throw new IllegalArgumentException("Unable to find user defined filter class", e);
			}
		}
		
		return operators;
	}
	
	/**
	 * Perform the user defined filters on the given stream and stored tuple
	 * @param streamTuple
	 * @param storedTuple
	 * @param filters
	 * @return
	 */
	public static boolean doUserDefinedFilterMatch(final Tuple streamTuple,
			final Map<UserDefinedFilter, byte[]> filters) {
				
		for(final Entry<UserDefinedFilter, byte[]> entry : filters.entrySet()) {
			
			final UserDefinedFilter operator = entry.getKey();
			final byte[] value = entry.getValue();
			
			final boolean result 
				= operator.filterTuple(streamTuple, value);
			
			if(! result) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Perform the user defined filters on the given stream and stored tuple
	 * @param streamTuple
	 * @param storedTuple
	 * @param filters
	 * @return
	 */
	public static boolean doUserDefinedFilterMatch(final Tuple streamTuple, 
			final Tuple storedTuple, final Map<UserDefinedFilter, byte[]> filters) {
				
		for(final Entry<UserDefinedFilter, byte[]> entry : filters.entrySet()) {
			
			final UserDefinedFilter operator = entry.getKey();
			final byte[] value = entry.getValue();
			
			final boolean result 
				= operator.filterJoinCandidate(streamTuple, storedTuple, value);
			
			if(! result) {
				return false;
			}
		}
		
		return true;
	}

}

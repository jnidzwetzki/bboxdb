/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.storage.queryprocessor.operator.join;

import java.util.Iterator;

import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.operator.SpatialIndexReadOperator;

public class FilterSpatialOperator extends SpatialIterator {

	/**
	 * The user defined filter
	 */
	private final UserDefinedFilter userDefinedFilter;
	
	/** 
	 * The user defined value
	 */
	private final String userDefinedValue;

	public FilterSpatialOperator(final Iterator<JoinedTuple> tupleStreamSource, 
			final SpatialIndexReadOperator indexReader, final UserDefinedFilter userDefinedFilter, 
			final String userDefinedValue) {
		
		super(tupleStreamSource, indexReader);
		this.userDefinedFilter = userDefinedFilter;
		this.userDefinedValue = userDefinedValue;
	}
	
	@Override
	protected JoinedTuple buildNextJoinedTuple(Tuple nextCandidateTuple) {
		final JoinedTuple tuple = super.buildNextJoinedTuple(nextCandidateTuple);
		
		// Pass tuple to the user defined filter
		if(userDefinedFilter.filterJoinCandidate(tuple.getTuple(0), tuple.getTuple(1), userDefinedValue)) {
			return tuple;
		} else {
			return null;
		}
	}

}

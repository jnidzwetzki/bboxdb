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
package org.bboxdb.query.queryprocessor.operator.join;

import java.util.Iterator;
import java.util.List;

import org.bboxdb.misc.Const;
import org.bboxdb.query.filter.UserDefinedFilter;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.queryprocessor.operator.SpatialIndexReadOperator;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterSpatialOperator extends SpatialIterator {

	/**
	 * The user defined filter
	 */
	private final List<UserDefinedFilterDefinition> udfs;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FilterSpatialOperator.class);


	public FilterSpatialOperator(final Iterator<MultiTuple> tupleStreamSource, 
			final SpatialIndexReadOperator indexReader, final List<UserDefinedFilterDefinition> udfs) {
		
		super(tupleStreamSource, indexReader);
		this.udfs = udfs;
	}

	@Override
	protected MultiTuple buildNextJoinedTuple(Tuple nextCandidateTuple) {
		final MultiTuple tuple = super.buildNextJoinedTuple(nextCandidateTuple);
		
		if(tuple == null) {
			return null;
		}
		
		try {
			// Pass tuple to the user defined filters
			for(final UserDefinedFilterDefinition udf : udfs) {
				final Class<?> filterClass = Class.forName(udf.getUserDefinedFilterClass());
				final UserDefinedFilter userDefinedFilter = (UserDefinedFilter) filterClass.newInstance();
				
				final boolean match = userDefinedFilter.filterJoinCandidate(tuple.getTuple(0), tuple.getTuple(1), udf.getUserDefinedFilterValue().getBytes(Const.DEFAULT_CHARSET));
				
				if(! match) {
					return null;
				}
			}
		} catch (Exception e) {
			logger.error("Exception while applying UDF", e);
			return null;
		} 
		
		return tuple;
	}

}

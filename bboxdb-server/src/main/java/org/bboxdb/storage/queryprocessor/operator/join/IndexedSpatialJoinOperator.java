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

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.network.query.filter.UserDefinedFilter;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.queryprocessor.operator.Operator;
import org.bboxdb.storage.queryprocessor.operator.SpatialIndexReadOperator;

public class IndexedSpatialJoinOperator implements Operator {

	/**
	 * The tuple stream operator
	 */
	private final Operator tupleStreamOperator;
	
	/**
	 * The index reader
	 */
	private final SpatialIndexReadOperator indexReader;

	/**
	 * The user defined filter clas
	 */
	private final String userDefinedFilterClass;

	/**
	 * The user defined filter value
	 */
	private final String userDefinedFilterValue;
	
	public IndexedSpatialJoinOperator(final Operator tupleStreamOperator, 
			final SpatialIndexReadOperator indexReader) {
		
		this(tupleStreamOperator, indexReader, "", "");
	}

	public IndexedSpatialJoinOperator(final Operator tupleStreamOperator, 
			final SpatialIndexReadOperator indexReader, final String userDefinedFilterClass,
			final String userDefinedFilterValue) {

		this.tupleStreamOperator = tupleStreamOperator;
		this.indexReader = indexReader;
		this.userDefinedFilterClass = userDefinedFilterClass;
		this.userDefinedFilterValue = userDefinedFilterValue;
	}

	/**
	 * Close all iterators
	 */
	@Override
	public void close() {
		CloseableHelper.closeWithoutException(tupleStreamOperator);
	}
	
	/**
	 * Get the query processing result
	 * @return
	 */
	public Iterator<JoinedTuple> iterator() {
		final Iterator<JoinedTuple> iterator = tupleStreamOperator.iterator();

		if(userDefinedFilterClass.equals("")) {
			return new SpatialIterator(iterator, indexReader);
		}
		
		try {
			final Class<?> filterClass = Class.forName(userDefinedFilterClass);
			final UserDefinedFilter userDefinedFilter = (UserDefinedFilter) filterClass.newInstance();
			
			return new FilterSpatialOperator(iterator, indexReader, 
					userDefinedFilter, userDefinedFilterValue);
		} catch (Exception e) {
			throw new IllegalArgumentException("Unable to load user defined filter", e);
		} 
	}
}

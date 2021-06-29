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
package org.bboxdb.query.queryprocessor.operator.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.queryprocessor.operator.Operator;
import org.bboxdb.query.queryprocessor.operator.SpatialIndexReadOperator;
import org.bboxdb.storage.entity.MultiTuple;

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
	 * The user defined filter classes
	 */
	private final List<UserDefinedFilterDefinition> udfs;
	
	public IndexedSpatialJoinOperator(final Operator tupleStreamOperator, 
			final SpatialIndexReadOperator indexReader) {
		
		this(tupleStreamOperator, indexReader, new ArrayList<>());
	}

	public IndexedSpatialJoinOperator(final Operator tupleStreamOperator, 
			final SpatialIndexReadOperator indexReader, final List<UserDefinedFilterDefinition> udfs) {

		this.tupleStreamOperator = tupleStreamOperator;
		this.indexReader = indexReader;
		this.udfs = udfs;
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
	public Iterator<MultiTuple> iterator() {
		final Iterator<MultiTuple> iterator = tupleStreamOperator.iterator();

		if(udfs.isEmpty()) {
			return new SpatialIterator(iterator, indexReader);
		}
		
		try {
			return new FilterSpatialOperator(iterator, indexReader, udfs);
		} catch (Exception e) {
			throw new IllegalArgumentException("Unable to load user defined filter", e);
		} 
	}
}

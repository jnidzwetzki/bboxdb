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
	final SpatialIndexReadOperator indexReader;

	public IndexedSpatialJoinOperator(final Operator tupleStreamOperator, 
			final SpatialIndexReadOperator indexReader) {

		this.tupleStreamOperator = tupleStreamOperator;
		this.indexReader = indexReader;
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
		return new Spatialterator(tupleStreamOperator.iterator(), indexReader);	
	}
}

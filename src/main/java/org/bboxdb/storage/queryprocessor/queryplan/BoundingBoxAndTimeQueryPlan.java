/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.storage.queryprocessor.queryplan;

import java.util.Iterator;

import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.datasource.DataSource;
import org.bboxdb.storage.queryprocessor.datasource.SpatialIndexDataSource;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateFilterIterator;

public class BoundingBoxAndTimeQueryPlan implements QueryPlan {

	/**
	 * The bounding box for the query
	 */
	protected final BoundingBox boundingBox;
	
	/**
	 * The timestamp for the query
	 * 
	 * @param boundingBox
	 * @param timestamp
	 */
	protected final long timestamp;
	
	public BoundingBoxAndTimeQueryPlan(final BoundingBox boundingBox, final long timestamp) {
		this.boundingBox = boundingBox;
		this.timestamp = timestamp;
	}

	@Override
	public Iterator<Tuple> execute(ReadOnlyTupleStorage readOnlyTupleStorage) {
		final DataSource dataSource = new SpatialIndexDataSource(readOnlyTupleStorage, boundingBox);
		
		final NewerAsTimePredicate predicate = new NewerAsTimePredicate(timestamp);
		
		final PredicateFilterIterator predicateFilterIterator 
			= new PredicateFilterIterator(dataSource.iterator(), predicate);
		
		return predicateFilterIterator;
	}
}

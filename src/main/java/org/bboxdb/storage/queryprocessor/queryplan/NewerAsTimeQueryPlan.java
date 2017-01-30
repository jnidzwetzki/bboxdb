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
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.datasource.DataSource;
import org.bboxdb.storage.queryprocessor.datasource.FullStoreScanSource;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateFilterIterator;

public class NewerAsTimeQueryPlan implements QueryPlan {
	
	/**
	 * The timestamp for the predicate
	 */
	protected final long timestamp;

	public NewerAsTimeQueryPlan(final long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public Iterator<Tuple> execute(final ReadOnlyTupleStorage readOnlyTupleStorage) {
		final DataSource fullStoreScanSource = new FullStoreScanSource(readOnlyTupleStorage);
		
		final NewerAsTimePredicate predicate = new NewerAsTimePredicate(timestamp);
		
		final PredicateFilterIterator predicateFilterIterator 
			= new PredicateFilterIterator(fullStoreScanSource.iterator(), predicate);
		
		return predicateFilterIterator;
	}

}

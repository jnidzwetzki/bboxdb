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
package org.bboxdb.storage.queryprocessor.operator;

import java.io.IOException;
import java.util.Iterator;

import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsVersionTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateJoinedTupleFilterIterator;

public class NewerAsVersionTimeSelectionOperator implements Operator {
	
	/**
	 * The timestamp for the predicate
	 */
	protected final long timestamp;
	
	/**
	 * The operator
	 */
	private Operator operator;

	public NewerAsVersionTimeSelectionOperator(final long timestamp, final Operator operator) {
		this.timestamp = timestamp;
		this.operator = operator;
	}

	@Override
	public Iterator<JoinedTuple> iterator() {
		final Predicate predicate = new NewerAsVersionTimePredicate(timestamp);
		return new PredicateJoinedTupleFilterIterator(operator.iterator(), predicate);		
	}

	@Override
	public void close() throws IOException {
		operator.close();
	}

}

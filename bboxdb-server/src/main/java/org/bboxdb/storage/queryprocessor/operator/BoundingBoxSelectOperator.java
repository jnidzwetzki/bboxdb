/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.queryprocessor.predicate.IntersectsBoundingBoxPredicate;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateJoinedTupleFilterIterator;

public class BoundingBoxSelectOperator implements Operator {

	/**
	 * The bounding box for the query
	 */
	private final Hyperrectangle boundingBox;
	
	/**
	 * The operator
	 */
	private final Operator parentOpeator;
	
	public BoundingBoxSelectOperator(final Hyperrectangle boundingBox, final Operator parentOperator) {
		this.boundingBox = boundingBox;
		this.parentOpeator = parentOperator;
	}

	@Override
	public Iterator<MultiTuple> iterator() {
		final Predicate predicate = new IntersectsBoundingBoxPredicate(boundingBox);
		return new PredicateJoinedTupleFilterIterator(parentOpeator.iterator(), predicate);		
	}

	@Override
	public void close() throws IOException {
		parentOpeator.close();
	}
}

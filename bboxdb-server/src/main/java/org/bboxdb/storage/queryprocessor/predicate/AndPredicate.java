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
package org.bboxdb.storage.queryprocessor.predicate;

import org.bboxdb.storage.entity.Tuple;

public class AndPredicate implements Predicate {

	/**
	 * The first predicate
	 */
	protected Predicate predicate1;
	
	/**
	 * The second predicate
	 */
	protected Predicate predicate2;

	public AndPredicate(final Predicate predicate1, final Predicate predicate2) {
		this.predicate1 = predicate1;
		this.predicate2 = predicate2;
	}

	@Override
	public boolean matches(final Tuple tuple) {
		return predicate1.matches(tuple) && predicate2.matches(tuple);
	}

}

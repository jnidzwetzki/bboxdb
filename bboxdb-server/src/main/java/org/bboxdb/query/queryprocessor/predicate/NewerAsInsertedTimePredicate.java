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
package org.bboxdb.query.queryprocessor.predicate;

import org.bboxdb.storage.entity.Tuple;

public class NewerAsInsertedTimePredicate implements Predicate {

	/**
	 * The time to compare
	 */
	private final long time;
	
	public NewerAsInsertedTimePredicate(final long time) {
		this.time = time;
	}

	@Override
	public boolean matches(final Tuple tuple) {
		if(tuple.getReceivedTimestamp() > time) {
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() {
		return "NewerAsTimePredicate [time=" + time + "]";
	}

}

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

package org.bboxdb.network.query.filter;

import org.bboxdb.storage.entity.Tuple;

public interface UserDefinedFilter {
	
	/**
	 * Filter a single tuple via UDF
	 * 
	 * @param tuple - the tuple to filter
	 * @param customData - custom data to execute the operation
	 * 
	 * @return true or false
	 */
	public default boolean filterTuple(final Tuple tuple, final byte[] customData) {
		throw new UnsupportedOperationException("The filterTuple method is not implemented");
	}
	
	/**
	 * Filter a join candidate via UDF
	 * 
	 * @param tuple1 - The first tuple of the join candidate
	 * @param tuple2 - The second tuple of the join candidate
	 * @param customData - custom data to execute the operation
	 * 
	 * @return true or false
	 */
	public default boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, 
			final byte[] customData) {
		
		throw new UnsupportedOperationException("The filterTuple method is not implemented");
	}
	
}

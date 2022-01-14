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
package org.bboxdb.query.filter;

import org.bboxdb.storage.entity.Tuple;

public class UserDefinedKeyFilter implements UserDefinedFilter {
	
	private String customDataString = null;

	@Override
	public boolean filterTuple(final Tuple tuple, final byte[] customData) {
		
		if(customDataString == null) {
			customDataString = new String(customData);
		}
		
		return tuple.getKey().equals(customDataString);
	}

	@Override
	public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final byte[] customData) {
		
		if(customDataString == null) {
			customDataString = new String(customData);
		}

		return tuple1.getKey().equals(customDataString) && tuple2.getKey().equals(customDataString);
	}
}

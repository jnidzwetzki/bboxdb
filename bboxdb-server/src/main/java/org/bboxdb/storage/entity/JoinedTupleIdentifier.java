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
package org.bboxdb.storage.entity;

import java.util.List;

public class JoinedTupleIdentifier implements EntityIdentifier {

	/**
	 * The tuples
	 */
	private List<Tuple> tuples;
	
	/**
	 * The tuple store names
	 */
	private List<String> tupleStoreNames;

	public JoinedTupleIdentifier(final List<Tuple> tuples, final List<String> tupleStoreNames) {
		this.tuples = tuples;
		this.tupleStoreNames = tupleStoreNames;
	}

	@Override
	public String toString() {
		return "JoinedTupleIdentifier [tuples=" + tuples + ", tupleStoreNames=" + tupleStoreNames + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tupleStoreNames == null) ? 0 : tupleStoreNames.hashCode());
		result = prime * result + ((tuples == null) ? 0 : tuples.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JoinedTupleIdentifier other = (JoinedTupleIdentifier) obj;
		if (tupleStoreNames == null) {
			if (other.tupleStoreNames != null)
				return false;
		} else if (!tupleStoreNames.equals(other.tupleStoreNames))
			return false;
		if (tuples == null) {
			if (other.tuples != null)
				return false;
		} else if (!tuples.equals(other.tuples))
			return false;
		return true;
	}
}

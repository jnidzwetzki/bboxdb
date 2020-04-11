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
package org.bboxdb.storage.entity;

import java.util.List;
import java.util.stream.Collectors;

public class JoinedTupleIdentifier implements EntityIdentifier {

	/**
	 * The tuples
	 */
	private List<EntityIdentifier> tupleIdentifier;
	
	/**
	 * The tuple store names
	 */
	private List<String> tupleStoreNames;

	public JoinedTupleIdentifier(final JoinedTuple joinedTuple) {
		this.tupleStoreNames = joinedTuple.getTupleStoreNames();
		
		// Only keep the getEntityIdentifiers in memory
		this.tupleIdentifier = joinedTuple.getTuples()
					.stream()
					.map(t -> t.getEntityIdentifier())
					.collect(Collectors.toList());
	}

	@Override
	public String toString() {
		return "JoinedTupleIdentifier [tuples=" + tupleIdentifier + ", tupleStoreNames=" + tupleStoreNames + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tupleStoreNames == null) ? 0 : tupleStoreNames.hashCode());
		result = prime * result + ((tupleIdentifier == null) ? 0 : tupleIdentifier.hashCode());
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
		if (tupleIdentifier == null) {
			if (other.tupleIdentifier != null)
				return false;
		} else if (!tupleIdentifier.equals(other.tupleIdentifier))
			return false;
		return true;
	}
}

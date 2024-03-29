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
package org.bboxdb.storage.entity;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JoinedTupleIdentifier implements EntityIdentifier {
	
	public enum Strategy {
		FULL, // Key, Version, Table
		KEY_AND_TABLE, // Key, Table
		FIRST_KEY_AND_TABLE, // 1st key and table
	}

	/**
	 * The tuples
	 */
	private final List<Object> tupleIdentifier;
	
	/**
	 * The tuple store names
	 */
	private List<String> tupleStoreNames;
	
	public JoinedTupleIdentifier(final MultiTuple joinedTuple) {
		this(joinedTuple, Strategy.FULL);
	}

	public JoinedTupleIdentifier(final MultiTuple joinedTuple, final Strategy strategy) {
		
		this.tupleStoreNames = joinedTuple.getTupleStoreNames();
		
		switch(strategy) {
			case FULL:
				this.tupleIdentifier = buildFullIdenfifierList(joinedTuple);
				break;

			case KEY_AND_TABLE:
				this.tupleIdentifier = buildKeyIdentifierList(joinedTuple);
				break;
				
			case FIRST_KEY_AND_TABLE:
				this.tupleIdentifier = Arrays.asList(joinedTuple.getTuple(0).getKey());
				break;
				
			default:
				throw new RuntimeException("Unkown strategy: " + strategy);
		}
		
	}

	/**
	 * Build the key identifier list
	 */
	private List<Object> buildKeyIdentifierList(final MultiTuple joinedTuple) {
		return joinedTuple.getTuples()
				.stream()
				.map(t -> t.getKey())
				.collect(Collectors.toList());
	}

	/**
	 * Build the full identifier list
	 * @param joinedTuple
	 * @return
	 */
	private List<Object> buildFullIdenfifierList(final MultiTuple joinedTuple) {
		// Only keep the getEntityIdentifiers in memory
		return joinedTuple.getTuples()
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

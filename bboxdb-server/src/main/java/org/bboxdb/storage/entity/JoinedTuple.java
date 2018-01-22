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

public class JoinedTuple {
	/**
	 * The joined tuples
	 */
	private final List<Tuple> tuples;
	
	/**
	 * The tuple store names
	 */
	private final List<String> tupleStoreNames;
	
	public JoinedTuple(final List<Tuple> tuples, final List<String> tupleStoreNames) {
		
		if(tuples.size() != tupleStoreNames.size()) {
			throw new IllegalArgumentException("Unable to create joined tuple with different argument sizes");
		}
		
		if(tuples.size() == 1) {
			throw new IllegalArgumentException("Unable to create joined tuple with only one tuple");
		}
		
		this.tuples = tuples;
		this.tupleStoreNames = tupleStoreNames;
	}

	@Override
	public String toString() {
		return "JoinedTuple [tuples=" + tuples + ", tupleStoreNames=" + tupleStoreNames + "]";
	}

	public Tuple getTuple(final int tupleNumber) {
		return tuples.get(tupleNumber);
	}

	public String getTupleStoreNames(final int tupleNumber) {
		return tupleStoreNames.get(0);
	}
	
}

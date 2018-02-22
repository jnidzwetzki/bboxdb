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

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.BoundingBox;

public class JoinedTuple implements Comparable<JoinedTuple>, PagedTransferableEntity {
	
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
		
		this.tuples = tuples;
		this.tupleStoreNames = tupleStoreNames;
	}
	
	public JoinedTuple(final Tuple tuple, final String tupleStoreName) {
		this.tuples = new ArrayList<>();
		this.tupleStoreNames = new ArrayList<>();
		
		this.tuples.add(tuple);
		this.tupleStoreNames.add(tupleStoreName);
	}

	@Override
	public String toString() {
		return "JoinedTuple [tuples=" + tuples + ", tupleStoreNames=" + tupleStoreNames + "]";
	}

	/**
	 * Get the tuple for the given position
	 * @param tupleNumber
	 * @return
	 */
	public Tuple getTuple(final int tupleNumber) {
		return tuples.get(tupleNumber);
	}

	/**
	 * Get the tuple store names
	 * @param tupleNumber
	 * @return
	 */
	public String getTupleStoreName(final int tupleNumber) {
		return tupleStoreNames.get(tupleNumber);
	}
	
	/**
	 * Get all tuples
	 * @return
	 */
	public List<Tuple> getTuples() {
		return new ArrayList<Tuple>(tuples);
	}
	
	/**
	 * Get all tuple store names
	 * @return
	 */
	public List<String> getTupleStoreNames() {
		return new ArrayList<String>(tupleStoreNames);
	}
	
	/**
	 * Return the number of tuples
	 * @return
	 */
	public int getNumberOfTuples() {
		return tuples.size();
	}
	
	/**
	 * Get the intersection bounding box of all tuples
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		BoundingBox intersectionBox = tuples.get(0).getBoundingBox();
				
		for(int i = 1; i < tuples.size(); i++) {
			final BoundingBox tupleBox = tuples.get(i).getBoundingBox();
			intersectionBox = intersectionBox.getIntersection(tupleBox);
		}
		
		return intersectionBox;
	}
	
	/**
	 * Convert the joined tuple into a single tuple if possible
	 * @return
	 */
	public Tuple convertToSingleTupleIfPossible() {
		if(tuples.size() != 1) {
			throw new IllegalArgumentException("Unable to convert to single tuple, size is: " 
					+ tuples.size());
		}
		
		return tuples.get(0);
	}

	@Override
	public int compareTo(final JoinedTuple o) {
		final int elements = Math.min(o.getNumberOfTuples(), getNumberOfTuples());
		
		for(int i = 0; i < elements; i++) {
			final int result = getTuple(i).compareTo(o.getTuple(i));
			if(result != 0) {
				return result;
			}
		}
		
		return getNumberOfTuples() - o.getNumberOfTuples();
	}
	
	/**
	 * Get the formated string
	 * @return
	 */
	public String getFormatedString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("===============\n");
		sb.append("Joined bounding box: " + getBoundingBox().toCompactString() + "\n\n");
		
		for(int i = 0; i < getNumberOfTuples(); i++) {
			sb.append("Table: " + getTupleStoreName(i) + "\n");
			sb.append("Tuple: " + getTuple(i).getFormatedString());
			
			if(i + 1 < getNumberOfTuples()) {
				 sb.append("\n");
			}
		}
		
		sb.append("===============\n");
	
		return sb.toString();
	}

	@Override
	public EntityIdentifier getEntityIdentifier() {
		return new JoinedTupleIdentifier(tuples, tupleStoreNames);
	}
	
}

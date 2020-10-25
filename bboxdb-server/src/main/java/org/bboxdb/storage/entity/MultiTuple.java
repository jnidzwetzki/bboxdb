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

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;

public class MultiTuple implements Comparable<MultiTuple>, PagedTransferableEntity {
	
	/**
	 * The joined tuples
	 */
	private final List<Tuple> tuples;
	
	/**
	 * The tuple store names
	 */
	private final List<String> tupleStoreNames;
	
	public MultiTuple(final List<Tuple> tuples, final List<String> tupleStoreNames) {
		if(tuples.size() != tupleStoreNames.size()) {
			throw new IllegalArgumentException("Unable to create joined tuple with different argument sizes");
		}
		
		this.tuples = tuples;
		this.tupleStoreNames = tupleStoreNames;
	}
	
	public MultiTuple(final Tuple tuple, final String tupleStoreName) {
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
	 * Get the bounding box of all tuples
	 * @return
	 */
	public Hyperrectangle getBoundingBox() {
		
		final List<Hyperrectangle> bboxes = new ArrayList<>();
				
		for(int i = 0; i < tuples.size(); i++) {
			final Hyperrectangle tupleBox = tuples.get(i).getBoundingBox();
			bboxes.add(tupleBox);
		}
		
		return Hyperrectangle.getCoveringBox(bboxes);
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
	public int compareTo(final MultiTuple o) {
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
		return new JoinedTupleIdentifier(this);
	}
	
	/**
	 * Get the most recent version of the joined tuple
	 * @return 
	 */
	public long getVersionTimestamp() {
		long result = Long.MIN_VALUE;
		for(final Tuple tuple : tuples) {
			result = Math.max(result, tuple.getVersionTimestamp());
		}
		
		return result;
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
		MultiTuple other = (MultiTuple) obj;
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

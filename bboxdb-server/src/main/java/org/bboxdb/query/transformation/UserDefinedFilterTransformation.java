/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.query.transformation;

import org.bboxdb.network.entity.TupleAndBoundingBox;
import org.bboxdb.query.filter.UserDefinedFilter;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;

public class UserDefinedFilterTransformation implements TupleTransformation {
	
	public final static String DELEMITER = "_____";

	/**
	 * The key to filter
	 */
	private final UserDefinedFilterDefinition userDefinedFilterDefinition;
	
	/**
	 * The user defined filter cache
	 */
	private UserDefinedFilter filter = null;
	
	public UserDefinedFilterTransformation(final UserDefinedFilterDefinition userDefinedFilterDefinition) {
		this.userDefinedFilterDefinition = userDefinedFilterDefinition;
	}

	public UserDefinedFilterTransformation(final String data) {
		final String[] splittedData = data.split(DELEMITER);
		
		if(splittedData.length != 2) {
			throw new IllegalArgumentException("Unable to split " + data + " into two parts");
		}
		
		this.userDefinedFilterDefinition = new UserDefinedFilterDefinition(splittedData[0], splittedData[1]);
	}


	@Override
	public TupleAndBoundingBox apply(final TupleAndBoundingBox input) {
		
		if(filter == null) {
			try {
				final Class<?> className = Class.forName(userDefinedFilterDefinition.getUserDefinedFilterClass());
				filter = (UserDefinedFilter) className.newInstance();
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to instance user defined fitler", e);
			} 
		}
		
		final byte[] value = userDefinedFilterDefinition.getUserDefinedFilterValue().getBytes();
		
		// Filter input
		if(filter.filterTuple(input.getTuple(), value)) {
			return input;
		}
		
		return null;
	}

	@Override
	public String getSerializedData() {
		return userDefinedFilterDefinition.getUserDefinedFilterClass() 
				+ DELEMITER + userDefinedFilterDefinition.getUserDefinedFilterValue();
	}

	@Override
	public String toString() {
		return "UserDefinedFilterTransformation [userDefinedFilterDefinition=" + userDefinedFilterDefinition + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userDefinedFilterDefinition == null) ? 0 : userDefinedFilterDefinition.hashCode());
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
		UserDefinedFilterTransformation other = (UserDefinedFilterTransformation) obj;
		if (userDefinedFilterDefinition == null) {
			if (other.userDefinedFilterDefinition != null)
				return false;
		} else if (!userDefinedFilterDefinition.equals(other.userDefinedFilterDefinition))
			return false;
		return true;
	}
	
}

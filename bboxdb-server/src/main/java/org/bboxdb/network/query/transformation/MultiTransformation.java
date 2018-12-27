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
package org.bboxdb.network.query.transformation;

import java.util.List;

import org.bboxdb.network.query.entity.TupleAndBoundingBox;

public class MultiTransformation implements TupleTransformation {

	/**
	 * The transformations
	 */
	private final List<TupleTransformation> transformations;

	public MultiTransformation(final List<TupleTransformation> transformations) {
		this.transformations = transformations;
	}

	@Override
	public TupleAndBoundingBox apply(final TupleAndBoundingBox input) {
		
		TupleAndBoundingBox result = input;
		
		for(final TupleTransformation transformation : transformations) {
			result = transformation.apply(result);
			
			if(result == null) {
				return null;
			}
		}
		
		return result;
	}

	@Override
	public String toString() {
		return "MultiTransformation [transformations=" + transformations + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((transformations == null) ? 0 : transformations.hashCode());
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
		MultiTransformation other = (MultiTransformation) obj;
		if (transformations == null) {
			if (other.transformations != null)
				return false;
		} else if (!transformations.equals(other.transformations))
			return false;
		return true;
	}
	
}
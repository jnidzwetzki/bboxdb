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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.entity.TupleAndBoundingBox;

public class BoundingBoxFilterTransformation implements TupleTransformation {

	/**
	 * The bounding box to filter
	 */
	private final Hyperrectangle hyperrectangle;

	public BoundingBoxFilterTransformation(final Hyperrectangle hyperrectangle) {
		this.hyperrectangle = hyperrectangle;
	}
	
	public BoundingBoxFilterTransformation(final String compactBox) {
		this.hyperrectangle = Hyperrectangle.fromString(compactBox);
	}

	@Override
	public String toString() {
		return "BoundingBoxFilterTransformation [hyperrectangle=" + hyperrectangle + "]";
	}

	@Override
	public TupleAndBoundingBox apply(final TupleAndBoundingBox input) {
		
		if(input.getTuple().getBoundingBox().intersects(hyperrectangle)) {
			return input;
		}
		
		return null;		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hyperrectangle == null) ? 0 : hyperrectangle.hashCode());
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
		BoundingBoxFilterTransformation other = (BoundingBoxFilterTransformation) obj;
		if (hyperrectangle == null) {
			if (other.hyperrectangle != null)
				return false;
		} else if (!hyperrectangle.equals(other.hyperrectangle))
			return false;
		return true;
	}

	@Override
	public String getSerializedData() {
		return hyperrectangle.toCompactString();
	}
	
}

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
package org.bboxdb.network.query.transformation;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.entity.TupleAndBoundingBox;

public class EnlargeBoundingBoxByFactorTransformation implements TupleTransformation {
	
	/**
	 * The enlargement factor
	 */
	private final double factor;

	public EnlargeBoundingBoxByFactorTransformation(final double factor) {
		this.factor = factor;
	}
	
	public EnlargeBoundingBoxByFactorTransformation(final String factor) throws InputParseException {
		this.factor = MathUtil.tryParseDouble(factor, () -> "Unable to parse: " + factor);
	}

	@Override
	public TupleAndBoundingBox apply(final TupleAndBoundingBox input) {
		
		final Hyperrectangle inputBox = input.getBoundingBox();
		final Hyperrectangle resultBox = inputBox.enlargeByFactor(factor);	
		
		return new TupleAndBoundingBox(input.getTuple(), resultBox);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(factor);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		EnlargeBoundingBoxByFactorTransformation other = (EnlargeBoundingBoxByFactorTransformation) obj;
		if (Double.doubleToLongBits(factor) != Double.doubleToLongBits(other.factor))
			return false;
		return true;
	}

	@Override
	public String getSerializedData() {
		return Double.toString(factor);
	}
	
}

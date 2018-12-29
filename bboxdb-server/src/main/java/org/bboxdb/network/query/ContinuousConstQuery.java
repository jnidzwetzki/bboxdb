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
package org.bboxdb.network.query;

import java.util.List;
import java.util.Objects;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.transformation.TupleTransformation;

public class ContinuousConstQuery extends AbstractContinuousQueryPlan {

	/**
	 * The hyperrectangle to compare
	 */
	private final Hyperrectangle compareRectangle;

	public ContinuousConstQuery(final String streamTable,
			final List<TupleTransformation> streamTransformation,
			final Hyperrectangle queryRectangle,
			final Hyperrectangle compareRectangle,
			final boolean reportPositiveNegative) {
		
			super(streamTable, streamTransformation, queryRectangle, reportPositiveNegative);
			this.compareRectangle = Objects.requireNonNull(compareRectangle);
	}

	/** 
	 * Get the compare rectangle
	 */
	public Hyperrectangle getCompareRectangle() {
		return compareRectangle;
	}

	@Override
	public String toString() {
		return "ContinuousConstQuery [compareRectangle=" + compareRectangle + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((compareRectangle == null) ? 0 : compareRectangle.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ContinuousConstQuery other = (ContinuousConstQuery) obj;
		if (compareRectangle == null) {
			if (other.compareRectangle != null)
				return false;
		} else if (!compareRectangle.equals(other.compareRectangle))
			return false;
		return true;
	}
	
}

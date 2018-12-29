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

public class ContinuousTableQueryPlan extends ContinuousQueryPlan {

	/** 
	 * The transformations of the tuples in the table
	 */
	private final List<TupleTransformation> tableTransformation;
	
	public ContinuousTableQueryPlan(final String streamTable,
			final List<TupleTransformation> streamTransformation,
			final Hyperrectangle queryRectangle,
			final List<TupleTransformation> tableTransformation,
			final boolean reportPositiveNegative) {
		
			super(streamTable, streamTransformation, queryRectangle, reportPositiveNegative);

			this.tableTransformation = Objects.requireNonNull(tableTransformation);
	}

	/**
	 * Get the table transformations
	 * @return
	 */
	public List<TupleTransformation> getTableTransformation() {
		return tableTransformation;
	}

	@Override
	public String toString() {
		return "ContinuousTableQuery [tableTransformation=" + tableTransformation + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((tableTransformation == null) ? 0 : tableTransformation.hashCode());
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
		ContinuousTableQueryPlan other = (ContinuousTableQueryPlan) obj;
		if (tableTransformation == null) {
			if (other.tableTransformation != null)
				return false;
		} else if (!tableTransformation.equals(other.tableTransformation))
			return false;
		return true;
	}
	
}

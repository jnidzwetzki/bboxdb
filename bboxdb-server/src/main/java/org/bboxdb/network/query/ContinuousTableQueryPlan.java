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
import org.bboxdb.network.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.network.query.transformation.TupleTransformation;

public class ContinuousTableQueryPlan extends ContinuousQueryPlan {

	/** 
	 * The transformations of the tuples in the table
	 */
	private final List<TupleTransformation> tableTransformation;
	
	/**
	 * The after join user defined filter
	 */
	private List<UserDefinedFilterDefinition> afterJoinFilter;
	
	/**
	 * The join table
	 */
	private final String joinTable;
	
	public ContinuousTableQueryPlan(final String streamTable,
			final String joinTable,
			final List<TupleTransformation> streamTransformation,
			final Hyperrectangle queryRectangle,
			final List<TupleTransformation> tableTransformation,
			final List<UserDefinedFilterDefinition> afterJoinFilter,
			final boolean reportPositiveNegative) {
		
			super(streamTable, streamTransformation, queryRectangle, reportPositiveNegative);
			this.afterJoinFilter = Objects.requireNonNull(afterJoinFilter);
			this.tableTransformation = Objects.requireNonNull(tableTransformation);
			this.joinTable = Objects.requireNonNull(joinTable);
	}

	/**
	 * Get the table transformations
	 * @return
	 */
	public List<TupleTransformation> getTableTransformation() {
		return tableTransformation;
	}

	/**
	 * Get the after join filter
	 * @return
	 */
	public List<UserDefinedFilterDefinition> getAfterJoinFilter() {
		return afterJoinFilter;
	}
	
	/**
	 * Get the join table
	 */
	public String getJoinTable() {
		return joinTable;
	}

	@Override
	public String toString() {
		return "ContinuousTableQueryPlan [tableTransformation=" + tableTransformation + ", afterJoinFilter="
				+ afterJoinFilter + ", joinTable=" + joinTable + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((afterJoinFilter == null) ? 0 : afterJoinFilter.hashCode());
		result = prime * result + ((joinTable == null) ? 0 : joinTable.hashCode());
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
		if (afterJoinFilter == null) {
			if (other.afterJoinFilter != null)
				return false;
		} else if (!afterJoinFilter.equals(other.afterJoinFilter))
			return false;
		if (joinTable == null) {
			if (other.joinTable != null)
				return false;
		} else if (!joinTable.equals(other.joinTable))
			return false;
		if (tableTransformation == null) {
			if (other.tableTransformation != null)
				return false;
		} else if (!tableTransformation.equals(other.tableTransformation))
			return false;
		return true;
	}
}

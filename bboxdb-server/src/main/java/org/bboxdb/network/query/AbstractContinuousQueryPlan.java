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

import org.bboxdb.network.query.transformation.TupleTransformation;

public abstract class AbstractContinuousQueryPlan {

	/**
	 * The stream table
	 */
	protected final String streamTable;
	
	/**
	 * The transformations of the stream entry
	 */
	protected final List<TupleTransformation> streamTransformation;
	
	/**
	 * The table name to query
	 */
	protected final String tableName;
	
	/**
	 * Report positive or negative elements to the user
	 */
	protected final boolean reportPositiveNegative;

	public AbstractContinuousQueryPlan(final String streamTable, final List<TupleTransformation> streamTransformation, 
			final String tableName, final boolean reportPositiveNegative) {
		
		this.streamTable = streamTable;
		this.streamTransformation = streamTransformation;
		this.tableName = tableName;
		this.reportPositiveNegative = reportPositiveNegative;
	}

	public String getStreamTable() {
		return streamTable;
	}

	public List<TupleTransformation> getStreamTransformation() {
		return streamTransformation;
	}

	public String getTableName() {
		return tableName;
	}

	public boolean isReportPositiveNegative() {
		return reportPositiveNegative;
	}

	@Override
	public String toString() {
		return "ContinuousQueryPlan [streamTable=" + streamTable + ", streamTransformation=" + streamTransformation
				+ ", tableName=" + tableName + ", reportPositiveNegative=" + reportPositiveNegative + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (reportPositiveNegative ? 1231 : 1237);
		result = prime * result + ((streamTable == null) ? 0 : streamTable.hashCode());
		result = prime * result + ((streamTransformation == null) ? 0 : streamTransformation.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
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
		AbstractContinuousQueryPlan other = (AbstractContinuousQueryPlan) obj;
		if (reportPositiveNegative != other.reportPositiveNegative)
			return false;
		if (streamTable == null) {
			if (other.streamTable != null)
				return false;
		} else if (!streamTable.equals(other.streamTable))
			return false;
		if (streamTransformation == null) {
			if (other.streamTransformation != null)
				return false;
		} else if (!streamTransformation.equals(other.streamTransformation))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}	
}

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
package org.bboxdb.network.query;

import java.util.List;
import java.util.Objects;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.transformation.TupleTransformation;

public abstract class ContinuousQueryPlan {

	/**
	 * The stream table
	 */
	private final String streamTable;
	
	/**
	 * The transformations of the stream entry
	 */
	private final List<TupleTransformation> streamTransformation;

	/**
	 * The query range
	 */
	private Hyperrectangle queryRange;

	public ContinuousQueryPlan(final String streamTable, final List<TupleTransformation> streamTransformation, 
			final Hyperrectangle queryRange) {
		
		this.streamTable = Objects.requireNonNull(streamTable);
		this.streamTransformation = Objects.requireNonNull(streamTransformation);
		this.queryRange = Objects.requireNonNull(queryRange);
	}

	public String getStreamTable() {
		return streamTable;
	}

	public List<TupleTransformation> getStreamTransformation() {
		return streamTransformation;
	}

	public Hyperrectangle getQueryRange() {
		return queryRange;
	}

	@Override
	public String toString() {
		return "ContinuousQueryPlan [streamTable=" + streamTable + ", streamTransformation=" + streamTransformation
				+ ", queryRange=" + queryRange + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queryRange == null) ? 0 : queryRange.hashCode());
		result = prime * result + ((streamTable == null) ? 0 : streamTable.hashCode());
		result = prime * result + ((streamTransformation == null) ? 0 : streamTransformation.hashCode());
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
		ContinuousQueryPlan other = (ContinuousQueryPlan) obj;
		if (queryRange == null) {
			if (other.queryRange != null)
				return false;
		} else if (!queryRange.equals(other.queryRange))
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
		return true;
	}

}

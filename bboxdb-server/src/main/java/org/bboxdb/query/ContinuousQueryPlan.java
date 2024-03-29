/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.query;

import java.util.List;
import java.util.Objects;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.transformation.TupleTransformation;

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
	 * The range filters
	 */
	private final List<UserDefinedFilterDefinition> streamFilters;
	
	/**
	 * The query UUID
	 */
	private final String queryUUID;

	/**
	 * The query range
	 */
	private final Hyperrectangle queryRange;
	
	/**
	 * This query should receive watermarks
	 */
	private final boolean receiveWatermarks;
	
	/**
	 * This query should receive invalidations
	 */
	private final boolean receiveInvalidations;

	/**
	 * Invalidate the query state after n watermarks
	 */
	private long invalidateStateAfterWatermarks;

	public ContinuousQueryPlan(final String queryUUID, final String streamTable, 
			final List<TupleTransformation> streamTransformation, 
			final Hyperrectangle queryRange, 
			final List<UserDefinedFilterDefinition> streamFilters,
			final boolean receiveWatermarks, final boolean receiveInvalidations,
			final long invalidateStateAfterWatermarks) {
		
		this.queryUUID = queryUUID;
		this.streamTable = Objects.requireNonNull(streamTable);
		this.streamTransformation = Objects.requireNonNull(streamTransformation);
		this.queryRange = Objects.requireNonNull(queryRange);
		this.streamFilters = Objects.requireNonNull(streamFilters);
		this.receiveWatermarks = receiveWatermarks;
		this.receiveInvalidations = receiveInvalidations;
		this.invalidateStateAfterWatermarks = invalidateStateAfterWatermarks;
	}
	
	public String getQueryUUID() {
		return queryUUID;
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

	public List<UserDefinedFilterDefinition> getStreamFilters() {
		return streamFilters;
	}
	
	public boolean isReceiveInvalidations() {
		return receiveInvalidations;
	}
	
	public boolean isReceiveWatermarks() {
		return receiveWatermarks;
	}

	public long getInvalidateStateAfterWatermarks() {
		return invalidateStateAfterWatermarks;
	}

	@Override
	public String toString() {
		return "ContinuousQueryPlan [streamTable=" + streamTable + ", streamTransformation=" + streamTransformation
				+ ", streamFilters=" + streamFilters + ", queryUUID=" + queryUUID + ", queryRange=" + queryRange
				+ ", receiveWatermarks=" + receiveWatermarks + ", receiveInvalidations=" + receiveInvalidations
				+ ", invalidateStateAfterWatermarks=" + invalidateStateAfterWatermarks + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (invalidateStateAfterWatermarks ^ (invalidateStateAfterWatermarks >>> 32));
		result = prime * result + ((queryRange == null) ? 0 : queryRange.hashCode());
		result = prime * result + ((queryUUID == null) ? 0 : queryUUID.hashCode());
		result = prime * result + (receiveInvalidations ? 1231 : 1237);
		result = prime * result + (receiveWatermarks ? 1231 : 1237);
		result = prime * result + ((streamFilters == null) ? 0 : streamFilters.hashCode());
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
		if (invalidateStateAfterWatermarks != other.invalidateStateAfterWatermarks)
			return false;
		if (queryRange == null) {
			if (other.queryRange != null)
				return false;
		} else if (!queryRange.equals(other.queryRange))
			return false;
		if (queryUUID == null) {
			if (other.queryUUID != null)
				return false;
		} else if (!queryUUID.equals(other.queryUUID))
			return false;
		if (receiveInvalidations != other.receiveInvalidations)
			return false;
		if (receiveWatermarks != other.receiveWatermarks)
			return false;
		if (streamFilters == null) {
			if (other.streamFilters != null)
				return false;
		} else if (!streamFilters.equals(other.streamFilters))
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

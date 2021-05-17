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
package org.bboxdb.network.client;

import java.util.HashSet;
import java.util.Set;

import org.bboxdb.network.query.ContinuousQueryPlan;

public class ContinuousQueryState {
	
	/**
	 * The query plan
	 */
	private final ContinuousQueryPlan queryPlan;
	
	/**
	 * Nodes where the query is registered
	 */
	private final Set<Long> registeredRegions;

	public ContinuousQueryState(final ContinuousQueryPlan queryPlan) {
		this(queryPlan, new HashSet<>());
	}
	
	public ContinuousQueryState(final ContinuousQueryPlan queryPlan, 
			final Set<Long> registeredRegions) {
		
		this.queryPlan = queryPlan;
		this.registeredRegions = registeredRegions;
	}

	/**
	 * Get the current query plan
	 * @return
	 */
	public ContinuousQueryPlan getQueryPlan() {
		return queryPlan;
	}

	/**
	 * The registered regions
	 * @return
	 */
	public Set<Long> getRegisteredRegions() {
		return registeredRegions;
	}

	@Override
	public String toString() {
		return "ContinousQueryState [queryPlan=" + queryPlan + ", registeredRegions=" + registeredRegions + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queryPlan == null) ? 0 : queryPlan.hashCode());
		result = prime * result + ((registeredRegions == null) ? 0 : registeredRegions.hashCode());
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
		ContinuousQueryState other = (ContinuousQueryState) obj;
		if (queryPlan == null) {
			if (other.queryPlan != null)
				return false;
		} else if (!queryPlan.equals(other.queryPlan))
			return false;
		if (registeredRegions == null) {
			if (other.registeredRegions != null)
				return false;
		} else if (!registeredRegions.equals(other.registeredRegions))
			return false;
		return true;
	}
	
}

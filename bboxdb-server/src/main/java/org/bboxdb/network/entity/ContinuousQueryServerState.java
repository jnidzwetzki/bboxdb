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
package org.bboxdb.network.entity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ContinuousQueryServerState {
	
	/**
	 * The active range query state
	 */
	private final Map<String, Set<String>> globalActiveRangeQueryElements;
	
	/**
	 * The active join elements
	 */
	private final Map<String, Map<String, Set<String>>> globalActiveJoinElements;
	
	public ContinuousQueryServerState() {
		this.globalActiveRangeQueryElements = new HashMap<>();
		this.globalActiveJoinElements = new HashMap<>();
	}
	
	/**
	 * Add the given range query to the global state
	 * @param queryUUID
	 * @param activeRangeQueryElements
	 */
	public void addRangeQueryState(final String queryUUID, final Set<String> activeRangeQueryElements) {
		globalActiveRangeQueryElements.put(queryUUID, new HashSet<>());		
		globalActiveRangeQueryElements.get(queryUUID).addAll(activeRangeQueryElements);
	}
	
	
	/**
	 * Add the given join query to the global state
	 * @param queryUUID
	 * @param activeJoinElements
	 */
	public void addJoinQueryState(final String queryUUID, final Map<String, Set<String>> activeJoinElements) {
		globalActiveJoinElements.put(queryUUID, new HashMap<>());
		globalActiveJoinElements.get(queryUUID).putAll(activeJoinElements);
	}
	
	/***
	 * Get the global active join elements
	 * @return
	 */
	public Map<String, Map<String, Set<String>>> getGlobalActiveJoinElements() {
		return globalActiveJoinElements;
	}
	
	/**
	 * Get the global active range query elements
	 * @return
	 */
	public Map<String, Set<String>> getGlobalActiveRangeQueryElements() {
		return globalActiveRangeQueryElements;
	}

	@Override
	public String toString() {
		return "ContinuousQueryServerState [globalActiveRangeQueryElements=" + globalActiveRangeQueryElements
				+ ", globalActiveJoinElements=" + globalActiveJoinElements + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((globalActiveJoinElements == null) ? 0 : globalActiveJoinElements.hashCode());
		result = prime * result
				+ ((globalActiveRangeQueryElements == null) ? 0 : globalActiveRangeQueryElements.hashCode());
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
		ContinuousQueryServerState other = (ContinuousQueryServerState) obj;
		if (globalActiveJoinElements == null) {
			if (other.globalActiveJoinElements != null)
				return false;
		} else if (!globalActiveJoinElements.equals(other.globalActiveJoinElements))
			return false;
		if (globalActiveRangeQueryElements == null) {
			if (other.globalActiveRangeQueryElements != null)
				return false;
		} else if (!globalActiveRangeQueryElements.equals(other.globalActiveRangeQueryElements))
			return false;
		return true;
	}
	
}

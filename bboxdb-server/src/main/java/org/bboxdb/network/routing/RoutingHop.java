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
package org.bboxdb.network.routing;

import java.util.List;

import org.bboxdb.distribution.membership.BBoxDBInstance;

public class RoutingHop {

	/**
	 * The distributed instance
	 */
	protected final BBoxDBInstance distributedInstance;
	
	/**
	 * The distribution regions
	 */
	protected final List<Long> distributionRegions;

	public RoutingHop(final BBoxDBInstance distributedInstance, final List<Long> distributionRegions) {
		this.distributedInstance = distributedInstance;
		this.distributionRegions = distributionRegions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributedInstance == null) ? 0 : distributedInstance.hashCode());
		result = prime * result + ((distributionRegions == null) ? 0 : distributionRegions.hashCode());
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
		RoutingHop other = (RoutingHop) obj;
		if (distributedInstance == null) {
			if (other.distributedInstance != null)
				return false;
		} else if (!distributedInstance.equals(other.distributedInstance))
			return false;
		if (distributionRegions == null) {
			if (other.distributionRegions != null)
				return false;
		} else if (!distributionRegions.equals(other.distributionRegions))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RoutingHop [distributedInstance=" + distributedInstance + ", distributionRegions=" + distributionRegions
				+ "]";
	}

	public BBoxDBInstance getDistributedInstance() {
		return distributedInstance;
	}

	public List<Long> getDistributionRegions() {
		return distributionRegions;
	}

	/** 
	 * Add a ID to the routing
	 */
	public void addRegion(final long regionId) {
		distributionRegions.add(regionId);
	}

}

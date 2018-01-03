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
package org.bboxdb.distribution;

import org.bboxdb.distribution.membership.BBoxDBInstance;

public class OutdatedDistributionRegion {
	
	/**
	 * The distributed region which is outdated
	 */
	protected final DistributionRegion distributedRegion;
	
	/**
	 * The instance which contains the newest data
	 */
	protected final BBoxDBInstance newestInstance;
	
	/**
	 * The local version of the data
	 */
	protected final long localVersion;
	
	public OutdatedDistributionRegion(final DistributionRegion distributedRegion, final BBoxDBInstance newestInstance, final long localVersion) {
		this.distributedRegion = distributedRegion;
		this.newestInstance = newestInstance;
		this.localVersion = localVersion;
	}

	@Override
	public String toString() {
		return "OudatedDistributionRegion [distributedRegion=" + distributedRegion
				+ ", newestInstance=" + newestInstance + ", localVersion="
				+ localVersion + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((distributedRegion == null) ? 0 : distributedRegion
						.hashCode());
		result = prime * result + (int) (localVersion ^ (localVersion >>> 32));
		result = prime * result
				+ ((newestInstance == null) ? 0 : newestInstance.hashCode());
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
		OutdatedDistributionRegion other = (OutdatedDistributionRegion) obj;
		if (distributedRegion == null) {
			if (other.distributedRegion != null)
				return false;
		} else if (!distributedRegion.equals(other.distributedRegion))
			return false;
		if (localVersion != other.localVersion)
			return false;
		if (newestInstance == null) {
			if (other.newestInstance != null)
				return false;
		} else if (!newestInstance.equals(other.newestInstance))
			return false;
		return true;
	}

	/**
	 * The distributed region
	 * @return
	 */
	public DistributionRegion getDistributedRegion() {
		return distributedRegion;
	}

	/**
	 * The newest instance for the region
	 * @return
	 */
	public BBoxDBInstance getNewestInstance() {
		return newestInstance;
	}

	/**
	 * The local version of the outdated region
	 * @return
	 */
	public long getLocalVersion() {
		return localVersion;
	}
	
}
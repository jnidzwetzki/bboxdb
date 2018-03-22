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
package org.bboxdb.storage.tuplestore.manager;

class DistributionRegionEntity {
	
	/**
	 * The distribution group name
	 */
	private final String distributionGroupName;
	
	/**
	 * The region id
	 */
	private final long regionId;

	public DistributionRegionEntity(final String distributionGroupName, final long regionId) {
		this.distributionGroupName = distributionGroupName;
		this.regionId = regionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionGroupName == null) ? 0 : distributionGroupName.hashCode());
		result = prime * result + (int) (regionId ^ (regionId >>> 32));
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
		DistributionRegionEntity other = (DistributionRegionEntity) obj;
		if (distributionGroupName == null) {
			if (other.distributionGroupName != null)
				return false;
		} else if (!distributionGroupName.equals(other.distributionGroupName))
			return false;
		if (regionId != other.regionId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DistributionRegionEntity [distributionGroupName=" + distributionGroupName + ", regionId=" + regionId
				+ "]";
	}

	public String getDistributionGroupName() {
		return distributionGroupName;
	}

	public long getRegionId() {
		return regionId;
	}
}
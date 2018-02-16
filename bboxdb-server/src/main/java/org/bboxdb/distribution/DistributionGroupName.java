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

public class DistributionGroupName implements Comparable<DistributionGroupName> {

	/**
	 * The name of the distribution group (e.g. mygroup)
	 */
	protected final String fullname;

	/**
	 * The name for an invalid group
	 */
	public final static String INVALID_GROUPNAME = null;
	
	/**
	 * The name of the distribution group
	 */
	protected String groupname = INVALID_GROUPNAME;

	public DistributionGroupName(final String fullname) {
		this.fullname = fullname;
		splitTablename();
	}
	
	/**
	 * Is the fullname valid?
	 * @return
	 */
	public boolean isValid() {
		return (groupname != INVALID_GROUPNAME);
	}
	
	/**
	 * Split the tablename into the three components
	 * @return
	 */
	protected void splitTablename() {
		
		if(fullname == null) {
			return;
		}
		
		if(fullname.contains("_")) {
			return;
		}
		
		groupname = fullname;
	}

	public String getFullname() {
		return fullname;
	}

	@Override
	public String toString() {
		return fullname;
	}

	@Override
	public int compareTo(final DistributionGroupName otherDistributionGroup) {
		return fullname.compareTo(otherDistributionGroup.getFullname());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fullname == null) ? 0 : fullname.hashCode());
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
		DistributionGroupName other = (DistributionGroupName) obj;
		if (fullname == null) {
			if (other.fullname != null)
				return false;
		} else if (!fullname.equals(other.fullname))
			return false;
		return true;
	}	
}

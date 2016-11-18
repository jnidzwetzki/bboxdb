/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.distribution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;

public class DistributionGroupName implements Comparable<DistributionGroupName> {

	/**
	 * The name of the distribution group (e.g. 3_mygroup)
	 */
	protected final String fullname;
	
	/**
	 * The value for an invalid dimension
	 */
	public final static int INVALID_DIMENSION = -1;
	
	/**
	 * The dimension of the distribution group
	 */
	protected int dimension = INVALID_DIMENSION;
	
	/**
	 * The name for an invalid group
	 */
	public final static String INVALID_GROUPNAME = null;
	
	/**
	 * The name of the distribution group
	 */
	protected String groupname = INVALID_GROUPNAME;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableName.class);
	

	public DistributionGroupName(final String fullname) {
		super();
		this.fullname = fullname;
		splitTablename();
	}
	
	/**
	 * Is the fullname valid?
	 * @return
	 */
	public boolean isValid() {
		return (dimension != INVALID_DIMENSION) && (groupname != INVALID_GROUPNAME);
	}
	
	/**
	 * Split the tablename into the three components
	 * @return
	 */
	protected void splitTablename() {
		
		if(fullname == null) {
			return;
		}
		
		final String[] parts = fullname.split("_");
		
		if(parts.length != 2) {
			logger.warn("Got invalid groupname: "+ fullname);
			return;
		}
		
		try {
			dimension = Short.parseShort(parts[0]);
		} catch(NumberFormatException e) {
			logger.warn("Invalid dimension: " + parts[0]);
			return;
		}
		
		if(dimension <= 0) {
			logger.warn("Got invalid dimension: " + dimension);
			return;
		}
		
		groupname = parts[1];
	}

	public String getFullname() {
		return fullname;
	}

	public int getDimension() {
		return dimension;
	}

	public String getGroupname() {
		return groupname;
	}

	@Override
	public String toString() {
		return "DistributionGroupName [fullname=" + fullname + ", dimension="
				+ dimension + ", groupname=" + groupname + "]";
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

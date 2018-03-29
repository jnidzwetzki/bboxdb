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
package org.bboxdb.storage.entity;

import java.util.OptionalLong;

import org.bboxdb.commons.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleStoreName implements Comparable<TupleStoreName> {

	/**
	 * The full name of the table
	 * 
	 * Format: groupname_tablename_tablenumber
	 * 
	 * e.g. mydata_mytable2
	 * 
	 */
	private final String fullname;
	
	/**
	 * Is the tablename valid?
	 */
	private final boolean valid;
	
	/**
	 * The group of the table
	 */
	private String group;
	
	/**
	 * The name of the table
	 */
	private String tablename;
	
	/**
	 * The region id
	 */
	private OptionalLong regionid;
	
	/**
	 * The value for an invalid group
	 */
	public final static String INVALID_GROUP = null;
	
	/**
	 * The value for an invalid table
	 */
	public final static String INVALID_TABLENAME = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleStoreName.class);
	
	public TupleStoreName(final String fullname) {
		this.fullname = fullname;
		this.valid = splitTablename();
	}
	
	public TupleStoreName(final String distributionGroup, 
			final String tablename, final long regionid) {		
		
		this.fullname = distributionGroup + "_" + tablename + "_" + regionid;
		this.valid = true;
		this.group = distributionGroup;
		this.tablename = tablename;
		this.regionid = OptionalLong.of(regionid);
	}
	
	/**
	 * Clone this sstable name with another region id
	 * @param regionId
	 * @return
	 */
	public TupleStoreName cloneWithDifferntRegionId(final long regionId) {
		return new TupleStoreName(group, tablename, regionId);
	}
	
	/**
	 * Split the tablename into the three components
	 * @return
	 */
	private boolean splitTablename() {
		
		if(fullname == null) {
			return false;
		}
		
		final String[] parts = fullname.split("_");
		final long terminals = StringUtil.countCharOccurrence(fullname, '_');

		if(parts.length - 1 != terminals) {
			logger.warn("Got invalid tablename: {}", fullname);
			return false;
		}
		
		if(parts.length != 2 && parts.length != 3) {
			logger.warn("Got invalid tablename: {}", fullname);	
			return false;
		}
		
		group = parts[0];
		tablename = parts[1];
		
		if(group.length() == 0 || tablename.length() == 0) {
			logger.warn("Got invalid tablename: {}", fullname);
			return false;
		}
		
		regionid = OptionalLong.empty();
		
		if(parts.length == 3) {
			final String regionIdString = parts[2];

			if(regionIdString.length() == 0) {
				logger.warn("Got invalid tablename: {}", fullname);
				return false;
			}
			
			try {
				final long regionidLong = Long.parseLong(regionIdString);
				regionid = OptionalLong.of(regionidLong);
			} catch(NumberFormatException e) {
				logger.warn("Invalid tablenumber: {}", regionIdString);
				return false;
			}			
		} 
		
		return true;
	}
	
	/**
	 * Is the tablename valid?
	 * @return
	 */
	public boolean isValid() {
		return valid;
	}
	
	/**
	 * Is this a local or a distribution version of a sstable
	 * @return
	 */
	public boolean isDistributedTable() {
		return regionid.isPresent();
	}

	/**
	 * Get the group from the tablename
	 * @return
	 */
	public String getDistributionGroup() {
		if(! isValid()) {
			return INVALID_GROUP;
		}
		
		return group;
	}
	
	/**
	 * Get the name of the table without the nameprefix
	 * @return
	 */
	public String getFullnameWithoutPrefix() {
		return getDistributionGroup() + "_" + tablename;
	}
	
	/**
	 * Get the identifier from the tablename
	 * @return
	 */
	public String getTablename() {
		if(! isValid()) {
			return INVALID_TABLENAME;
		}
		
		return tablename;
	}
	
	/**
	 * Get the region id of the table
	 * @return
	 */
	public OptionalLong getRegionId() {
		return regionid;
	}
	
	/**
	 * Added getter for the fullname
	 * @return
	 */
	public String getFullname() {
		return fullname;
	}
	
	/**
	 * Get the bytes of the fullname
	 * @return
	 */
	public byte[] getFullnameBytes() {
		return fullname.getBytes();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fullname == null) ? 0 : fullname.hashCode());
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((regionid == null) ? 0 : regionid.hashCode());
		result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
		result = prime * result + (valid ? 1231 : 1237);
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
		TupleStoreName other = (TupleStoreName) obj;
		if (fullname == null) {
			if (other.fullname != null)
				return false;
		} else if (!fullname.equals(other.fullname))
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (regionid == null) {
			if (other.regionid != null)
				return false;
		} else if (!regionid.equals(other.regionid))
			return false;
		if (tablename == null) {
			if (other.tablename != null)
				return false;
		} else if (!tablename.equals(other.tablename))
			return false;
		if (valid != other.valid)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TupleStoreName [fullname=" + fullname + ", valid=" + valid + ", group=" + group + ", tablename="
				+ tablename + ", regionid=" + regionid + "]";
	}

	@Override
	public int compareTo(final TupleStoreName otherStoreName) {
		return fullname.compareTo(otherStoreName.getFullname());
	}
	
}

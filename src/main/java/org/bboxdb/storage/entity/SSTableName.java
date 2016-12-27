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
package org.bboxdb.storage.entity;

import org.bboxdb.distribution.DistributionGroupName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableName {

	/**
	 * The full name of the table
	 * 
	 * Format: dimension_groupname_tablename_tablenumber
	 * 
	 * e.g. 3_mydata_mytable2
	 * 
	 */
	protected final String fullname;
	
	/**
	 * Is the tablename valid?
	 */
	protected final boolean valid;
	
	/**
	 * The dimension of the table
	 */
	protected short dimension;
	
	/**
	 * The group of the table
	 */
	protected String group;
	
	/**
	 * The name of the table
	 */
	protected String tablename;
	
	/**
	 * The region id
	 */
	protected int regionid;
	
	/**
	 * The value for an invalid dimension
	 */
	public final static short INVALID_DIMENSION = -1;
	
	/**
	 * The value for an invalid group
	 */
	public final static String INVALID_GROUP = null;
	
	/**
	 * The value for an invalid table
	 */
	public final static String INVALID_TABLENAME = null;
	
	/**
	 * The value for an invalid name prefix
	 */
	public final static short INVALID_REGIONID = -1;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableName.class);
	
	public SSTableName(final String fullname) {
		super();
		this.fullname = fullname;
		this.valid = splitTablename();
	}
	
	public SSTableName(final short dimension, final String distributionGroup, 
			final String tablename, final int regionid) {
		super();
		
		this.fullname = dimension + "_" + distributionGroup + "_" + tablename + "_" + regionid;
		this.valid = true;

		this.dimension = dimension;
		this.group = distributionGroup;
		this.tablename = tablename;
		this.regionid = regionid;
	}
	
	/**
	 * Clone this sstable name with another region id
	 * @param regionId
	 * @return
	 */
	public SSTableName cloneWithDifferntRegionId(final int regionId) {
		return new SSTableName(dimension, group, tablename, regionId);
	}
	
	/**
	 * Split the tablename into the three components
	 * @return
	 */
	protected boolean splitTablename() {
		
		if(fullname == null) {
			return false;
		}
		
		final String[] parts = fullname.split("_");
		
		if(parts.length != 3 && parts.length != 4) {
			logger.warn("Got invalid tablename: " + fullname);
			
			final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
			for(final StackTraceElement stackTraceElement : stackTrace) {
				logger.warn(stackTraceElement.toString());

			}
			return false;
		}
		
		try {
			dimension = Short.parseShort(parts[0]);
		} catch(NumberFormatException e) {
			logger.warn("Invalid dimension: " + parts[0]);
			return false;
		}
		
		if(dimension <= 0) {
			logger.warn("Got invalid dimension: " + dimension);
			return false;
		}
		
		group = parts[1];
		tablename = parts[2];
		
		if(parts.length == 4) {
			try {
				regionid = Short.parseShort(parts[3]);
			} catch(NumberFormatException e) {
				logger.warn("Invalid tablenumber: " + parts[3]);
				return false;
			}			
		} else {
			regionid = INVALID_REGIONID;
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
		return regionid != INVALID_REGIONID;
	}

	/**
	 * Get the dimension from the tablename
	 * @return
	 */
	public short getDimension() {
		if(! isValid()) {
			return INVALID_DIMENSION;
		}
		
		return dimension;
	}
	
	/**
	 * Get the group from the tablename
	 * @return
	 */
	public String getGroup() {
		if(! isValid()) {
			return INVALID_GROUP;
		}
		
		return group;
	}
	
	/**
	 * Get the name of the distribution group
	 * @return
	 */
	public String getDistributionGroup() {
		return dimension + "_" + group;
	}
	
	/**
	 * Get the name of the table without the nameprefix
	 * @return
	 */
	public String getFullnameWithoutPrefix() {
		return getDistributionGroup() + "_" + tablename;
	}
	
	/**
	 * Get the distribution group as object
	 * @return
	 */
	public DistributionGroupName getDistributionGroupObject() {
		return new DistributionGroupName(getDistributionGroup());
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
	public int getRegionId() {
		return regionid;
	}
	
	/**
	 * Is the region id valid?
	 * @return
	 */
	public boolean isRegionIdValid() {
		return regionid != INVALID_REGIONID;
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
	public String toString() {
		return "SSTableName [fullname=" + fullname + ", valid=" + valid + ", dimension=" + dimension + ", group="
				+ group + ", tablename=" + tablename + ", regionid=" + regionid + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dimension;
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result
				+ ((tablename == null) ? 0 : tablename.hashCode());
		result = prime * result
				+ ((fullname == null) ? 0 : fullname.hashCode());
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
		SSTableName other = (SSTableName) obj;
		if (dimension != other.dimension)
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (tablename == null) {
			if (other.tablename != null)
				return false;
		} else if (!tablename.equals(other.tablename))
			return false;
		if (fullname == null) {
			if (other.fullname != null)
				return false;
		} else if (!fullname.equals(other.fullname))
			return false;
		if (valid != other.valid)
			return false;
		return true;
	}
	
}

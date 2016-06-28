package de.fernunihagen.dna.jkn.scalephant.storage.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableName {

	/**
	 * The full name of the table
	 * 
	 * Format: dimension_groupname_identifier
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
	 * The nameprefix
	 */
	protected short nameprefix;
	
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
	public final static short INVALID_NAMEPEFIX = -1;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableName.class);
	
	public SSTableName(final String fullname) {
		super();
		this.fullname = fullname;
		this.valid = splitTablename();
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
			logger.warn("Got invalid tablename: "+ fullname);
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
				nameprefix = Short.parseShort(parts[3]);
			} catch(NumberFormatException e) {
				logger.warn("Invalid name prefix: " + parts[3]);
				return false;
			}			
		} else {
			nameprefix = INVALID_NAMEPEFIX;
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
	 * Get the nameprefix of the table
	 * @return
	 */
	public short getNameprefix() {
		return nameprefix;
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
		return "Tablename [tablename=" + fullname + ", valid=" + valid
				+ ", dimension=" + dimension + ", group=" + group
				+ ", identifier=" + tablename + "]";
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

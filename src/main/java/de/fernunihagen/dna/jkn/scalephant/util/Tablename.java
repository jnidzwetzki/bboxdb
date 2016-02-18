package de.fernunihagen.dna.jkn.scalephant.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tablename {

	/**
	 * The name of the table
	 * 
	 * Format: dimension_groupname_identifier
	 * 
	 * e.g. 3_mydata_mytable2
	 * 
	 */
	protected final String tablename;
	
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
	 * The identifier of the table
	 */
	protected String identifier;
	
	/**
	 * The value for an invalid dimension
	 */
	public final static short INVALID_DIMENSION = -1;
	
	/**
	 * The value for an invalid group
	 */
	public final static String INVALID_GROUP = null;
	
	/**
	 * The value for an invalid identifier
	 */
	public final static String INVALID_IDENTIFIER = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Tablename.class);
	
	public Tablename(final String tablename) {
		super();
		this.tablename = tablename;
		this.valid = splitTablename();
	}
	
	/**
	 * Split the tablename into the three components
	 * @return
	 */
	protected boolean splitTablename() {
		
		if(tablename == null) {
			return false;
		}
		
		final String[] parts = tablename.split("_");
		
		if(parts.length != 3) {
			logger.warn("Got invalid tablename: "+ tablename);
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
		identifier = parts[2];
		
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
	 * Get the identifier from the tablename
	 * @return
	 */
	public String getIdentifier() {
		if(! isValid()) {
			return INVALID_IDENTIFIER;
		}
		
		return identifier;
	}

	@Override
	public String toString() {
		return "Tablename [tablename=" + tablename + ", valid=" + valid
				+ ", dimension=" + dimension + ", group=" + group
				+ ", identifier=" + identifier + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dimension;
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result
				+ ((identifier == null) ? 0 : identifier.hashCode());
		result = prime * result
				+ ((tablename == null) ? 0 : tablename.hashCode());
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
		Tablename other = (Tablename) obj;
		if (dimension != other.dimension)
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (identifier == null) {
			if (other.identifier != null)
				return false;
		} else if (!identifier.equals(other.identifier))
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
	
}

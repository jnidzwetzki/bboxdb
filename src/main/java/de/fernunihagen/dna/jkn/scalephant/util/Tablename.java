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
		final String[] parts = tablename.split("_");
		
		if(parts.length != 3) {
			logger.warn("Got invalid tablename: "+ tablename);
			return false;
		}
		
		dimension = Short.parseShort(parts[0]);
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
			return -1;
		}
		
		return dimension;
	}
	
	/**
	 * Get the group from the tablename
	 * @return
	 */
	public String getGroup() {
		if(! isValid()) {
			return null;
		}
		
		return group;
	}
	
	/**
	 * Get the identifier from the tablename
	 * @return
	 */
	public String getIdentifier() {
		if(! isValid()) {
			return null;
		}
		
		return identifier;
	}
	
	@Override
	public String toString() {
		return "Tablename [tablename=" + tablename + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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

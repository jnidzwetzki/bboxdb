package de.fernunihagen.dna.jkn.scalephant.distribution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.util.TablenameHelper;

public class DistributionGroupName {

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
	private final static Logger logger = LoggerFactory.getLogger(TablenameHelper.class);
	

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
	
}

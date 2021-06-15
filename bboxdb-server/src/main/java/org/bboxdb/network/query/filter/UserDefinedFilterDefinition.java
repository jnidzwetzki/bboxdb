package org.bboxdb.network.query.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserDefinedFilterDefinition {
	
	/**
	 * The user defined filter class
	 */
	private final String userDefinedFilterClass;
	
	/**
	 * The user defined filter value
	 */
	private final String userDefinedFilterValue;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(UserDefinedFilterDefinition.class);

	public UserDefinedFilterDefinition(final String userDefinedFilterClass, 
			final String userDefinedFilterValue) {
		
		if(userDefinedFilterClass.length() < 3) {
			logger.error("This is not a valid filter class: {}", userDefinedFilterClass);
		}
		
		this.userDefinedFilterClass = userDefinedFilterClass;
		this.userDefinedFilterValue = userDefinedFilterValue;
	}

	@Override
	public String toString() {
		return "UserDefinedFilterDefinition [userDefinedFilterClass=" + userDefinedFilterClass
				+ ", userDefinedFilterValue=" + userDefinedFilterValue + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userDefinedFilterClass == null) ? 0 : userDefinedFilterClass.hashCode());
		result = prime * result + ((userDefinedFilterValue == null) ? 0 : userDefinedFilterValue.hashCode());
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
		UserDefinedFilterDefinition other = (UserDefinedFilterDefinition) obj;
		if (userDefinedFilterClass == null) {
			if (other.userDefinedFilterClass != null)
				return false;
		} else if (!userDefinedFilterClass.equals(other.userDefinedFilterClass))
			return false;
		if (userDefinedFilterValue == null) {
			if (other.userDefinedFilterValue != null)
				return false;
		} else if (!userDefinedFilterValue.equals(other.userDefinedFilterValue))
			return false;
		return true;
	}

	public String getUserDefinedFilterClass() {
		return userDefinedFilterClass;
	}

	public String getUserDefinedFilterValue() {
		return userDefinedFilterValue;
	}
		
}

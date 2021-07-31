package org.bboxdb.network.server.query.continuous;

import java.util.Map;
import java.util.Set;

public class IdleQueryStateResult {
	
	/**
	 * The removed stream keys
	 */
	private final Set<String> removedStreamKeys;
	
	/**
	 * The removed join partners
	 */
	private final Map<String, Set<String>> removedJoinPartners;
		
	public IdleQueryStateResult(final Set<String> removedStreamKeys, 
			final Map<String, Set<String>> removedJoinPartners) {
		this.removedStreamKeys = removedStreamKeys;
		this.removedJoinPartners = removedJoinPartners;
	}

	public Set<String> getRemovedStreamKeys() {
		return removedStreamKeys;
	}
	
	public Map<String, Set<String>> getRemovedJoinPartners() {
		return removedJoinPartners;
	}
	
}
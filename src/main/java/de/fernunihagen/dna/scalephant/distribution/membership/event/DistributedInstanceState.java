package de.fernunihagen.dna.scalephant.distribution.membership.event;

public enum DistributedInstanceState {
	
	UNKNOWN("unknown"), 
	READONLY("readonly"), 
	READWRITE("readwrite");
	
	/**
	 * The zookeeper value
	 */
	private final String zookeeperValue;
	
	DistributedInstanceState(final String zookeeperValue) {
		this.zookeeperValue = zookeeperValue;
	}
	
	/**
	 * Get the zookeeper representation of the state
	 * @return
	 */
	public String getZookeeperValue() {
		return zookeeperValue;
	}

}

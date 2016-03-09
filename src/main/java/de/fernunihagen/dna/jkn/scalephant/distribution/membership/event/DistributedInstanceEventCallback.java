package de.fernunihagen.dna.jkn.scalephant.distribution.membership.event;

public interface DistributedInstanceEventCallback {
	
	/**
	 * A membership event occurred 
	 */
	public void distributedInstanceEvent(final DistributedInstanceEvent event);

}

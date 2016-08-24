package de.fernunihagen.dna.scalephant.distribution.membership.event;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

public class DistributedInstanceDeleteEvent extends DistributedInstanceEvent {

	public DistributedInstanceDeleteEvent(final DistributedInstance instance) {
		super(instance);
	}
	
	@Override
	public String toString() {
		return "DistributedInstanceDeleteEvent [instance=" + instance + "]";
	}

}

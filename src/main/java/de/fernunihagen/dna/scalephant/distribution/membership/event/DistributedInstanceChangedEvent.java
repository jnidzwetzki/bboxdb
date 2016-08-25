package de.fernunihagen.dna.scalephant.distribution.membership.event;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

public class DistributedInstanceChangedEvent extends DistributedInstanceEvent {

	public DistributedInstanceChangedEvent(final DistributedInstance instance) {
		super(instance);
	}

	@Override
	public String toString() {
		return "DistributedInstanceChangedEvent [instance=" + instance + "]";
	}

}

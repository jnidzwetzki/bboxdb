package de.fernunihagen.dna.scalephant.distribution.membership.event;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

public class DistributedInstanceAddEvent extends DistributedInstanceEvent {

	public DistributedInstanceAddEvent(final DistributedInstance instance) {
		super(instance);
	}

	@Override
	public String toString() {
		return "DistributedInstanceAddEvent [instance=" + instance + "]";
	}

}

package de.fernunihagen.dna.jkn.scalephant.distribution.membership.event;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public abstract class DistributedInstanceEvent {

	protected final DistributedInstance instance;

	public DistributedInstanceEvent(final DistributedInstance instance) {
		super();
		this.instance = instance;
	}
	
}

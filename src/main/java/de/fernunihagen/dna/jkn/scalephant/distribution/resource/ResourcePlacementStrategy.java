package de.fernunihagen.dna.jkn.scalephant.distribution.resource;

import java.util.Collection;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public interface ResourcePlacementStrategy {
	
	/**
	 * Find a system to use for ressource allocation
	 * @param systems
	 * @return
	 */
	public DistributedInstance findSystemToAllocate(Collection<DistributedInstance> systems);
	
}

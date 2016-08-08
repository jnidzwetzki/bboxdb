package de.fernunihagen.dna.jkn.scalephant.distribution.placement;

import java.util.Collection;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public interface ResourcePlacementStrategy {

	/**
	 * Get a set with distributed instances. These instances will be responsible for 
	 * a new resource.
	 * 
	 * @return
	 * @throws ResourceAllocationException 
	 */
	public List<DistributedInstance> getInstancesForNewRessource(final Collection<DistributedInstance> systems, final int numberToAllocate) throws ResourceAllocationException; 
	
}

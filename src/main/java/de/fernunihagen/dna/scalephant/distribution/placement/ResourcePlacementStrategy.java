package de.fernunihagen.dna.scalephant.distribution.placement;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

public abstract class ResourcePlacementStrategy {

	/**
	 * Get a set with distributed instances. These instances will be responsible for 
	 * a new resource. The systems from the blacklist are excluded.
	 * 
	 * @return
	 * @throws ResourceAllocationException 
	 */
	public abstract DistributedInstance getInstancesForNewRessource(final List<DistributedInstance> systems, final Collection<DistributedInstance> blacklist) throws ResourceAllocationException;

	/**
	 * Get a set with distributed instances. These instances will be responsible for 
	 * a new resource.
	 * 
	 * @return
	 * @throws ResourceAllocationException 
	 */
	public DistributedInstance getInstancesForNewRessource(final List<DistributedInstance> systems) throws ResourceAllocationException {
		return getInstancesForNewRessource(systems, new HashSet<DistributedInstance>());
	}

}

package de.fernunihagen.dna.scalephant.distribution.placement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

public class RoundRobinResourcePlacementStrategy extends ResourcePlacementStrategy {

	/**
	 * The last assigned instance
	 */
	protected DistributedInstance lastInstance;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RoundRobinResourcePlacementStrategy.class);
	
	public RoundRobinResourcePlacementStrategy() {
		lastInstance = null;
	}
	
	@Override
	public DistributedInstance getInstancesForNewRessource(final List<DistributedInstance> systems, final Collection<DistributedInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		final List<DistributedInstance> availableSystems = new ArrayList<DistributedInstance>(systems);
		availableSystems.removeAll(blacklist);

		PlacementHelper.removeAllNonReadySystems(availableSystems);
		
		if(availableSystems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, all systems are blacklisted");
		}
		
		// First resource allocation
		if(lastInstance == null) {
			lastInstance = availableSystems.get(0);
			return availableSystems.get(0);
		}

		// Last allocated system was removed or is not ready, start on position 0
		if(! availableSystems.contains(lastInstance)) {
			lastInstance = availableSystems.get(0);
			return availableSystems.get(0);
		}
		
		// Find the position of the last assignment
		int lastPosition = 0; 
		for(; lastPosition < availableSystems.size(); lastPosition++) {
			if(availableSystems.get(lastPosition).equals(lastInstance)) {
				break;
			}
		}
		
		final int nextPosition = (lastPosition + 1) % availableSystems.size();
		lastInstance = availableSystems.get(nextPosition);
		return lastInstance;
	}
}

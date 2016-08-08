package de.fernunihagen.dna.jkn.scalephant.distribution.placement;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.Const;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public class RandomResourcePlacementStrategy extends ResourcePlacementStrategy {

	/**
	 * The random generator
	 */
	protected final Random randomGenerator;
	
	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(RandomResourcePlacementStrategy.class);
	
	public RandomResourcePlacementStrategy() {
		randomGenerator = new Random();
	}
	
	@Override
	public DistributedInstance getInstancesForNewRessource(final List<DistributedInstance> systems, final Collection<DistributedInstance> blacklist) throws ResourceAllocationException {
		
		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		if(systems.size() == blacklist.size()) {
			throw new ResourceAllocationException("Unable to choose a system, size of blacklist and system list are equal");
		}
		
		int retryCounter = 0; 
		
		while(retryCounter < Const.OPERATION_RETRY) {
			final int element = Math.abs(randomGenerator.nextInt()) % systems.size();
			final DistributedInstance system = systems.get(element);
			
			if(! blacklist.contains(system)) {
				return system;
			}
			
			retryCounter++;
		}
		
		throw new ResourceAllocationException("Unable to pick a non blacklisted system by using " + Const.OPERATION_RETRY + " retries");
	}
}

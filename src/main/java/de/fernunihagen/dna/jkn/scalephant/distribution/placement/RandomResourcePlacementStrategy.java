package de.fernunihagen.dna.jkn.scalephant.distribution.placement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public class RandomResourcePlacementStrategy implements ResourcePlacementStrategy {

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
	public List<DistributedInstance> getInstancesForNewRessource(final Collection<DistributedInstance> systems, final int numberToAllocate) throws ResourceAllocationException {
		
		final List<DistributedInstance> resultSystems = new ArrayList<DistributedInstance>(numberToAllocate);

		if(systems.isEmpty()) {
			throw new ResourceAllocationException("Unable to choose a system, list of systems is empty");
		}
		
		if(numberToAllocate > systems.size()) {
			throw new ResourceAllocationException("Number to allocate " + numberToAllocate + " is larger than system list " + systems);
		}

		final List<DistributedInstance> elements = new ArrayList<DistributedInstance>(systems.size());
		
		synchronized (systems) {
			elements.addAll(systems);
		}
		
		for(short i = 0; i < numberToAllocate; i++) {
			final int element = Math.abs(randomGenerator.nextInt()) % elements.size();
			resultSystems.add(elements.get(element));
		}
		
		if(logger.isDebugEnabled()) {
			logger.debug("Assigning ressources: " + resultSystems);
		}
		
		return resultSystems;
	}
}

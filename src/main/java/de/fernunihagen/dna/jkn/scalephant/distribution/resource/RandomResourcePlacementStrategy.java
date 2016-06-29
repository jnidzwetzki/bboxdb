package de.fernunihagen.dna.jkn.scalephant.distribution.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;

public class RandomResourcePlacementStrategy implements ResourcePlacementStrategy {

	/**
	 * The random generator
	 */
	protected final Random randomGenerator;
	
	public RandomResourcePlacementStrategy() {
		randomGenerator = new Random();
	}
	
	@Override
	public DistributedInstance findSystemToAllocate(final Collection<DistributedInstance> systems) {

		synchronized (systems) {
			final List<DistributedInstance> elements = new ArrayList<DistributedInstance>(systems);
			final int element = Math.abs(randomGenerator.nextInt()) % elements.size();
			return elements.get(element);
		}
		
	}

}

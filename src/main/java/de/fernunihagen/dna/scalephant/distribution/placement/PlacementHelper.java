package de.fernunihagen.dna.scalephant.distribution.placement;

import java.util.Iterator;
import java.util.List;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.scalephant.distribution.membership.event.DistributedInstanceState;

public class PlacementHelper {

	/**
	 * Remove all non ready (state != READWRITE) systems 
	 * @param systems
	 */
	public static void removeAllNonReadySystems(final List<DistributedInstance> systems) {
		for(final Iterator<DistributedInstance> iter = systems.iterator(); iter.hasNext(); ) {
			final DistributedInstance system = iter.next();
			
			if(system.getState() != DistributedInstanceState.READWRITE) {
				iter.remove();
			}
		}
	}

}

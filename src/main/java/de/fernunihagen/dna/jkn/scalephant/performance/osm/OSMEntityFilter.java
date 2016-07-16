package de.fernunihagen.dna.jkn.scalephant.performance.osm;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public abstract class OSMEntityFilter {
	
	/**
	 * Does the node the filter pass or not 
	 */
	public abstract boolean forwardNode(final Node node);
}

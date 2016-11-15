package de.fernunihagen.dna.scalephant.performance.osm.filter;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public abstract class OSMSinglePointEntityFilter {
	
	/**
	 * Does the node the filter pass or not 
	 */
	public abstract boolean forwardNode(final Node node);
	
}

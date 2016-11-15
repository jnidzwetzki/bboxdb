package de.fernunihagen.dna.scalephant.performance.osm.filter;

import java.util.Collection;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public abstract class OSMMultiPointEntityFilter {
	
	/**
	 * Does the node the filter pass or not 
	 */
	public abstract boolean forwardNode(final Collection<Tag> tags);
	
}

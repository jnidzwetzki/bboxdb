package de.fernunihagen.dna.jkn.scalephant.performance.osm;

import java.util.Collection;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public class OSMRoadsEntityFilter extends OSMMultiPointEntityFilter {

	@Override
	public boolean forwardNode(final Collection<Tag> tags) {
		for(final Tag tag : tags) {
			if(tag.getKey().equals("highway")) {
				return true;
			}
		}
		
		return false;
	}

}

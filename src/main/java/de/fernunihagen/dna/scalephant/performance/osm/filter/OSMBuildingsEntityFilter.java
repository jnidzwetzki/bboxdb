package de.fernunihagen.dna.scalephant.performance.osm.filter;

import java.util.Collection;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public class OSMBuildingsEntityFilter extends OSMMultiPointEntityFilter {

	@Override
	public boolean forwardNode(final Collection<Tag> tags) {
		
		for(final Tag tag : tags) {
			System.out.println(tag);
			if(tag.getKey().equals("building") && tag.getValue().equals("yes")) {
				return true;
			}
		}
		
		return false;
	}

}

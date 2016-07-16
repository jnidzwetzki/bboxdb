package de.fernunihagen.dna.jkn.scalephant.performance.osm;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public class OSMTreeEntityFilter extends OSMEntityFilter {
	
	public boolean forwardNode(final Node node) {
		
		for(final Tag tag : node.getTags()) {
			//System.out.println(node.getId() + " " + tag.getKey() + " " + tag.getValue());	
			
			// Filter
			if(tag.getKey().equals("natural") && tag.getValue().equals("tree")) {
				return true;
			}		
		}
		
		return false;
	}

}

package de.fernunihagen.dna.scalephant.performance.osm;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public class OSMTrafficSignalEntityFilter extends OSMSinglePointEntityFilter {
	
	public boolean forwardNode(final Node node) {
		
		for(final Tag tag : node.getTags()) {
			//System.out.println(node.getId() + " " + tag.getKey() + " " + tag.getValue());	
			
			// Filter
			if(tag.getValue().equals("traffic_signals")) {
				return true;
			}		
		}
		
		return false;
	}

}

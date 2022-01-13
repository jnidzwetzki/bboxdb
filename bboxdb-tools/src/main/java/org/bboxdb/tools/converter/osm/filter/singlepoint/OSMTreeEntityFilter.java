/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.tools.converter.osm.filter.singlepoint;

import java.util.Collection;

import org.bboxdb.tools.converter.osm.filter.OSMTagEntityFilter;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

public class OSMTreeEntityFilter implements OSMTagEntityFilter {
	
	public boolean match(final Collection<Tag> tags) {
		
		for(final Tag tag : tags) {
			//System.out.println(node.getId() + " " + tag.getKey() + " " + tag.getValue());	
			
			// Filter
			if(tag.getKey().equals("natural") && tag.getValue().equals("tree")) {
				return true;
			}		
		}
		
		return false;
	}
	
	@Override
	public boolean isMultiPointFilter() {
		return false;
	}

}

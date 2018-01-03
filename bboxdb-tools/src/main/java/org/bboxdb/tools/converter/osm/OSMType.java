/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.tools.converter.osm;

public enum OSMType {
		
	// Single Point elements
	TREE("tree"),
	TRAFFIC_SIGNAL("trafficsignal"),
	
	// Multi Point elements
	ROAD("road"),
	BUILDING("building"),
	WATER("water"), 
	WOOD("wood");
	
	private final String name;

	OSMType(final String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	/**
	 * Construct from name
	 * @param name
	 * @return
	 */
	public static OSMType fromString(final String name) {
	    for (OSMType b : OSMType.values()) {
	      if (b.getName().equalsIgnoreCase(name)) {
	        return b;
	      }
	    }
	    return null;
	  }
}

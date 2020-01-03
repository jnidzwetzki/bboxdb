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
package org.bboxdb.tools.gui.views.query;

import java.awt.Color;
import java.util.Arrays;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.JoinedTupleIdentifier;
import org.bboxdb.storage.entity.Tuple;

public class OverlayElementHelper {
	
	/**
	 * Add the tuples to the overlay
	 * @param color1
	 * @param tuples
	 * @return 
	 */
	public static OverlayElement getOverlayElement(final Tuple tuple, final String tupleStoreName, 
			final Color color) {
		
		final String data = new String(tuple.getDataBytes());				
		final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(data);
		
		// Add also the table to the identifier
		final EntityIdentifier identifier = new JoinedTupleIdentifier(Arrays.asList(tuple), 
				Arrays.asList(tupleStoreName));
		
		final OverlayElement overlayElement = new OverlayElement(identifier, 
				tupleStoreName, polygon, color);
		
		return overlayElement;
	}
}

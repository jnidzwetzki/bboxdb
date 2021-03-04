/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.JoinedTupleIdentifier;
import org.bboxdb.storage.entity.Tuple;

public class OverlayElementBuilder {
	

	/**
	 * Create a new overlay element group from one tuple
	 * @param tuple
	 * @param tupleStoreName
	 * @param color
	 * @return
	 */
	public static OverlayElementGroup createOverlayElementGroup(final Tuple tuple, final String tupleStoreName,
			final Color color) {
		
		final OverlayElement overlayElement = generateOverlayElement(tuple, tupleStoreName, color);
		
		return new OverlayElementGroup(Arrays.asList(overlayElement));
	}
	
	/**
	 * Add the tuples to the overlay
	 * @param color1
	 * @param tuples
	 * @return 
	 */
	public static OverlayElementGroup createOverlayElementGroup(final MultiTuple joinedTuple, 
			final List<Color> colors) {

		final List<OverlayElement> elements = new ArrayList<>();
		
		for(int i = 0; i < joinedTuple.getNumberOfTuples(); i++) {
			
			final Tuple tuple = joinedTuple.getTuple(i);
			final String tupleStoreName = joinedTuple.getTupleStoreName(i);
			final Color color = colors.get(i % colors.size());
			
			final OverlayElement overlayElement = generateOverlayElement(tuple, tupleStoreName, color);
			
			elements.add(overlayElement);
		}
		
		// Draw points first
		elements.sort((c1, c2) -> Integer.compare(c2.getPolygon().getNumberOfPoints(), c1.getPolygon().getNumberOfPoints()));
		
		return new OverlayElementGroup(elements);
	}

	/**
	 * Generate a new overlayElemnt
	 * @param tuple
	 * @param tupleStoreName
	 * @param color
	 * @return
	 */
	private static OverlayElement generateOverlayElement(final Tuple tuple, final String tupleStoreName,
			final Color color) {
		
		final String data = new String(tuple.getDataBytes());				
		final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(data);
		
		// Add also the table to the identifier
		MultiTuple joinedTuple = new MultiTuple(Arrays.asList(tuple), 
				Arrays.asList(tupleStoreName));
		final EntityIdentifier identifier = new JoinedTupleIdentifier(joinedTuple);
		
		final OverlayElement overlayElement = new OverlayElement(identifier, 
				tupleStoreName, polygon, color);
		return overlayElement;
	}

}

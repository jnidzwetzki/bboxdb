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
package org.bboxdb.tools.converter.tuple;

import java.util.Map;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;

public class AuTransportGeoJSONTupleBuilder extends TupleBuilder {

	@Override
	public Tuple buildTuple(final String valueData, final String keyData) {

		final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(valueData);
		
		
		final Map<String, String> properties = polygon.getProperties();
		
		String key = keyData;

		if(properties.containsKey("TripID")) {
			key = properties.get("TripID").replace(" ", "_");
		} else {
			key = properties.get("RouteID").replace(" ", "_");
		}
		
		// Longitude / Latitude switch
		polygon.invertPolygonCoordinates();
		
		final byte[] tupleBytes = polygon.toGeoJson().getBytes();

		if(polygon.getBoundingBox().getDimension() == 0) {
			return null;
		}
		
		final Hyperrectangle bbox = polygon.getBoundingBox().enlargeByAmount(boxPadding);
				
		if(properties.containsKey("Timestamp")) {
			return new Tuple(key, bbox, tupleBytes, Long.parseLong(properties.get("Timestamp")));
		}
		
		return new Tuple(key, bbox, tupleBytes);
	}

}

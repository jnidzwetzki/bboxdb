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
package org.bboxdb.network.query.filter;

import org.bboxdb.storage.entity.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esri.core.geometry.MapOGCStructure;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

public class UserDefinedGeoJsonSpatialFilter implements UserDefinedFilter {

	/**
	 * The cached geometry
	 */
	private OGCGeometry customGeomety = null;
	
	/**
	 * The overlapping distance
	 */
	private final static double MAX_OVERLAPPING_POINT_DISTANCE_METER = 5;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(UserDefinedGeoJsonSpatialFilter.class);


	/**
	 * Perform a real filter based on the geometry of the data
	 */
	@Override
	public boolean filterTuple(final Tuple tuple, final byte[] customData) {
		
		// No custom geometry is passed
		if(customData == null) { 
			return true;
		}
		
		final String customString = new String(customData);
		
		final String geoJsonString = new String(tuple.getDataBytes());
		final JSONObject geoJsonObject = new JSONObject(geoJsonString);
		
		if(!customString.startsWith("{") && customString.contains(":")) {
			
			final String[] customParts = customString.split(":");

			if(customParts.length != 2) {
				logger.error("Unable to split {} into two parts", customString);
			}
			
			final String key = customParts[0];
			final String value = customParts[1];
						
			return containsProperty(geoJsonObject, key, value);
		}
		
		// Cache the custom geometry between method calls
		if(customGeomety == null) {
			customGeomety = geoJoinToGeomety(customString);
		}

		final OGCGeometry geometry = extractGeometry(geoJsonObject);

        return geometry.intersects(customGeomety);
	}
	
	/**
	 * Perform a real join based on the geometry of the data
	 */
	@Override
	public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final byte[] customData) {
		
		final String geoJsonString1 = new String(tuple1.getDataBytes());
		final String geoJsonString2 = new String(tuple2.getDataBytes());

		final JSONObject jsonObject1 = new JSONObject(geoJsonString1);
		final JSONObject jsonObject2 = new JSONObject(geoJsonString2);
		
		// Full text search on string (if provided)
		if(customData != null && customData.length > 1) {
			final String customDataString = new String(customData);
			final String[] customParts = customDataString.split(":");
			
			if(customParts.length != 2) {
				logger.error("Unable to split {} into two parts", customDataString);
			}
			
			final String key = customParts[0];
			final String value = customParts[1];
			
			if(! containsProperty(jsonObject1, key, value) && ! containsProperty(jsonObject2, key, value)) {
				return false;
			}
		}
		
		final OGCGeometry geometry1 = extractGeometry(jsonObject1);
		final OGCGeometry geometry2 = extractGeometry(jsonObject2);
		
		return performIntersectionTest(geometry1, geometry2);
	}

	/**
	 * Contains the given json a proper element in the map?
	 * @param json
	 * @param key
	 * @param value
	 * @return
	 */
	private boolean containsProperty(final JSONObject json, final String key, final String value) {
		final JSONObject properties = json.optJSONObject("properties");
		
		if(properties == null) {
			return false;
		}
		
		final String valueForKey = properties.optString(key);
		
		if(valueForKey == null) {
			return false;
		}
		
		return value.equals(valueForKey);
	}
	
	/**
	 * Perform the intersection test of the geometries
	 * 
	 * @param geometry1
	 * @param geometry2
	 * @return
	 */
	protected boolean performIntersectionTest(final OGCGeometry geometry1, final OGCGeometry geometry2) {
		if(geometry1 instanceof OGCPoint || geometry2 instanceof OGCPoint) {
			final double geometryDistrance = geometry1.distance(geometry2);

			final double latitude = getLatitudeFromGeometries(geometry1, geometry2);
			
		    //final double lat0 = Math.toRadians(latitudeLow);
		    final double lat0 = (latitude * (Math.PI) / 180);
			
		    final double equator_circumference = 6371000.0;
		    final double polar_circumference = 6356800.0;

		    final double m_per_deg_long = 360 / polar_circumference;
		    final double m_per_deg_lat = Math.abs(360 / (Math.cos(lat0) * equator_circumference));

		    final double deg_diff_lat = MAX_OVERLAPPING_POINT_DISTANCE_METER * m_per_deg_lat; 
		    final double deg_diff_long = MAX_OVERLAPPING_POINT_DISTANCE_METER * m_per_deg_long;
			
			return geometryDistrance < Math.max(deg_diff_lat, deg_diff_long);
		} else {
		    return geometry1.intersects(geometry2);
		}
	}
	
	/**
	 * Get the latitude from the geometries
	 * @param geometry1
	 * @param geometry2
	 * @return
	 */
	private double getLatitudeFromGeometries(final OGCGeometry geometry1, final OGCGeometry geometry2) {
	
		if(geometry1 instanceof OGCPoint) {
			final OGCPoint point1 = (OGCPoint) geometry1;
			return point1.X();
		} 
		
		if(geometry2 instanceof OGCPoint) {
			final OGCPoint point2 = (OGCPoint) geometry2;
			return point2.X();
		}
		
		logger.error("One of the provided geometries has to be a point");
		
		return 0;
	}

	/**
	 * Extract the geometry from the tuple
	 * @param tuple
	 * @return
	 */
	private OGCGeometry extractGeometry(final JSONObject jsonObject) {
		
		// Extract geometry (if exists)
		final JSONObject geometryObject = jsonObject.optJSONObject("geometry");
		
		if(geometryObject != null) {
			return geoJoinToGeomety(geometryObject.toString());
		}
		
		return geoJoinToGeomety(jsonObject.toString());
	}

	/**
	 * Convert the GeoJSON element to a ESRI geometry
	 * @param jsonString
	 * @return
	 */
	private OGCGeometry geoJoinToGeomety(final String jsonString) {
		
		final OperatorImportFromGeoJson op = (OperatorImportFromGeoJson) OperatorFactoryLocal
		        .getInstance().getOperator(Operator.Type.ImportFromGeoJson);
				
	    final MapOGCStructure structure = op.executeOGC(WktImportFlags.wktImportDefaults, jsonString, null);

	    return OGCGeometry.createFromOGCStructure(structure.m_ogcStructure,
	    		structure.m_spatialReference);
	}

}

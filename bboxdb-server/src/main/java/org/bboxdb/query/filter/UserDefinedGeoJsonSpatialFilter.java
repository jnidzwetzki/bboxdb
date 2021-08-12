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
package org.bboxdb.query.filter;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapOGCStructure;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Proximity2DResult;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

public class UserDefinedGeoJsonSpatialFilter implements UserDefinedFilter {

	/**
	 * The cached user provided geometry
	 */
	private OGCGeometry customGeomety = null;
	
	/**
	 * The cached user provided geometry bounding box
	 */
	private Hyperrectangle customGeometyBBox = null;
	
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
			final GeoJsonPolygon geoJsonPolygon = GeoJsonPolygon.fromGeoJson(customString);
			customGeometyBBox = geoJsonPolygon.getBoundingBox();
		}

		// If a custom geometry is available
		if(customGeomety != null) {
			
			if(customGeometyBBox.coversAtLeastOneDimensionComplete(tuple.getBoundingBox())) {
				return true;
			}
			
			final OGCGeometry geometry = extractGeometry(geoJsonObject);

	        return geometry.intersects(customGeomety);
		}
		
		return true;
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
				return false;
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
	 * Contains the given JSON a proper element in the map?
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
		
		if(geometry1 instanceof OGCPoint) {
			final OGCPoint point = (OGCPoint) geometry1;
			return isPointNearby(point, geometry2);
		}
		
		if(geometry2 instanceof OGCPoint) {
			final OGCPoint point = (OGCPoint) geometry2;
			return isPointNearby(point, geometry1);
		}
		
		
		return geometry1.intersects(geometry2);
	}
	
	/**
	 * Get the latitude from the geometries
	 * @param geometry1
	 * @param geometry2
	 * @return
	 */
	private boolean isPointNearby(final OGCPoint point, final OGCGeometry geometry) {
		final Point esriPoint = (Point) point.getEsriGeometry();
		
		final Proximity2DResult nearestCoordinate =
				GeometryEngine.getNearestCoordinate(geometry.getEsriGeometry(), esriPoint, true);
		final Point nearestPoint = nearestCoordinate.getCoordinate();
		
		final double distance = GeometryEngine.geodesicDistanceOnWGS84(esriPoint, nearestPoint);
				
		return distance <= MAX_OVERLAPPING_POINT_DISTANCE_METER;
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

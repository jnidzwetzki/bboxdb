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
package org.bboxdb.tools.converter.osm.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.storage.entity.BoundingBox;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class Polygon implements Serializable {

	/**
	 * The JSON constant for ID
	 */
	protected static final String JSON_ID = "id";
	
	/**
	 * The JSON constant for type
	 */
	protected static final String JSON_TYPE = "type";
	
	/**
	 * The JSON constant for properties
	 */
	protected static final String JSON_PROPERTIES = "properties";
	
	/**
	 * The JSON constant for geometry
	 */
	protected static final String JSON_GEOMETRY = "geometry";
	
	/**
	 * The JSON constant for coordinates
	 */
	protected static final String JSON_COORDINATES = "coordinates";
	
	/**
	 * The JSON constant for Feature
	 */
	protected static final String JSON_FEATURE = "Feature";

	/**
	 * 
	 */
	private static final long serialVersionUID = -25587980224359866L;

	/**
	 * The ID of the structure
	 */
	protected final long id;

	/**
	 * The list of our points
	 */
	protected final List<OSMPoint> pointList = new ArrayList<OSMPoint>();

	/**
	 * The map of properties
	 */
	protected final Map<String, String> properties = new HashMap<String, String>();

	public Polygon(final long id) {
		this.id = id;
	}

	/**
	 * Add a new point
	 * @param d
	 * @param e
	 */
	public void addPoint(final double d, final double e) {
		final OSMPoint point = new OSMPoint(d, e);
		pointList.add(point);
	}

	/**
	 * Add a new property
	 */
	public void addProperty(final String key, final String value) {
		properties.put(key, value);
	}

	/**
	 * Get the number of points
	 * @return 
	 */
	public int getNumberOfPoints() {
		return pointList.size();
	}

	/**
	 * Get the bounding box from the object
	 * @return
	 */
	public BoundingBox getBoundingBox() {

		if(pointList.isEmpty()) {
			return BoundingBox.EMPTY_BOX;
		}

		final OSMPoint firstPoint = pointList.get(0);
		double minX = firstPoint.getX();
		double maxX = firstPoint.getX();
		double minY = firstPoint.getY();
		double maxY = firstPoint.getY();

		for(final OSMPoint osmPoint : pointList) {
			minX = Math.min(minX, osmPoint.getX());
			maxX = Math.max(maxX, osmPoint.getX());
			minY = Math.min(minY, osmPoint.getY());
			maxY = Math.max(maxY, osmPoint.getY());
		}

		return new BoundingBox(minX, maxX, minY, maxY);
	}

	/**
	 * Get the ID of the polygon
	 * @return
	 */
	public long getId() {
		return id;
	}

	/**
	 * Return the GEO JSON representation of the polygon 
	 * @return
	 */
	public String toFormatedGeoJson() {
		final JSONObject featureJson = buildJSON();
		return featureJson.toString(3);
	}
	
	/**
	 * Return the GEO JSON representation of the polygon 
	 * @return
	 */
	public String toGeoJson() {
		final JSONObject featureJson = buildJSON();
		return featureJson.toString();
	}

	/**
	 * Build the JSON representation
	 * @return
	 */
	protected JSONObject buildJSON() {
		final JSONObject featureJson = new JSONObject();
		featureJson.put(JSON_TYPE, JSON_FEATURE);
		featureJson.put(JSON_ID, id);

		final JSONObject geometryJson = new JSONObject();
		final JSONArray coordinateJson = new JSONArray();
		
		geometryJson.put(JSON_COORDINATES, coordinateJson);
		featureJson.put(JSON_GEOMETRY, geometryJson);

		if(pointList.isEmpty()) {
			// Nothing to add
		} else if(pointList.size() == 1) {
			geometryJson.put(JSON_TYPE, "Point");
			coordinateJson.put(pointList.get(0).getX());
			coordinateJson.put(pointList.get(0).getY());
		} else {
			geometryJson.put(JSON_TYPE, "Polygon");
			
			for(OSMPoint point : pointList) {
				final JSONArray coordinatesJson = new JSONArray();
				coordinateJson.put(coordinatesJson);

				coordinatesJson.put(point.getX());
				coordinatesJson.put(point.getY());
			}
		}

		final JSONObject propertiesJson = new JSONObject();
		featureJson.put(JSON_PROPERTIES, propertiesJson);
		for(final String key : properties.keySet()) {
			propertiesJson.put(key, properties.get(key));
		}
		return featureJson;
	}
	
	/**
	 * Import an object from GeoJSON
	 * @return
	 */
	public static Polygon fromGeoJson(final String jsonData) {
		final JSONTokener tokener = new JSONTokener(jsonData);
		final JSONObject jsonObject = new JSONObject(tokener);
		final Long objectId = jsonObject.getLong(JSON_ID);

		final Polygon polygon = new Polygon(objectId);
		
		// Geometry
		final JSONObject geometry = jsonObject.getJSONObject(JSON_GEOMETRY);
		final JSONArray coordinates = geometry.getJSONArray(JSON_COORDINATES);
				
		if(coordinates.length() == 2 && coordinates.optJSONArray(0) == null) {
			// Point
			final double coordiante0 = coordinates.getDouble(0);
			final double coordiante1 = coordinates.getDouble(1);
			polygon.addPoint(coordiante0, coordiante1);
		} else if(coordinates.length() > 1) {
			// Polygon
			for(int i = 0; i < coordinates.length(); i++) {
				final JSONArray jsonArray = coordinates.getJSONArray(i);
				final double coordiante0 = jsonArray.getDouble(0);
				final double coordiante1 = jsonArray.getDouble(1);
				polygon.addPoint(coordiante0, coordiante1);
			}
		}
		
		// Properties
		final JSONObject properties = jsonObject.getJSONObject(JSON_PROPERTIES);
		for(final String key : properties.keySet()) {
			final String value = properties.getString(key);
			polygon.addProperty(key, value);
		}
		
		return polygon;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((pointList == null) ? 0 : pointList.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Polygon other = (Polygon) obj;
		if (id != other.id)
			return false;
		if (pointList == null) {
			if (other.pointList != null)
				return false;
		} else if (!pointList.equals(other.pointList))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Polygon [id=" + id + ", pointList=" + pointList + ", properties=" + properties + "]";
	}

	//=====================================================
	// Test * Test * Test * Test * Test
	//=====================================================
	public static void main(final String[] args) {
		System.out.println("=====================");
		final Polygon polygon = new Polygon(12);
		polygon.addPoint(23.1, 23.1);
		polygon.addPoint(21.1, 23.0);
		polygon.addPoint(3.1, 9.9);
		System.out.println(polygon.toFormatedGeoJson());
		System.out.println(polygon.toGeoJson());

		System.out.println("=====================");
		final Polygon polygon2 = new Polygon(14);
		polygon2.addPoint(23.1, 23.1);
		System.out.println(polygon2.toFormatedGeoJson());
		System.out.println(polygon2.toGeoJson());

		System.out.println("=====================");
		final Polygon polygon3 = new Polygon(15);
		System.out.println(polygon3.toFormatedGeoJson());
		System.out.println(polygon3.toGeoJson());
	}
}

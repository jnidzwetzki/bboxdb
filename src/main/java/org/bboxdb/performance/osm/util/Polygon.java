/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.performance.osm.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bboxdb.storage.entity.BoundingBox;
import org.json.JSONArray;
import org.json.JSONObject;

public class Polygon implements Serializable {

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
			maxX = Math.min(maxX, osmPoint.getX());
			minY = Math.min(minY, osmPoint.getY());
			maxY = Math.min(maxY, osmPoint.getY());
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
	public String toGeoJson() {

		final JSONObject featureJson = new JSONObject();
		featureJson.put("type", "Feature");
		featureJson.put("id", id);

		final JSONObject geometryJson = new JSONObject();

		final JSONArray coordinateJson = new JSONArray();
		geometryJson.put("coordinates", coordinateJson);
		featureJson.put("geometry", geometryJson);

		if(pointList.isEmpty()) {
			// Nothing to add
		} else if(pointList.size() == 1) {
			geometryJson.put("type", "Point");
			coordinateJson.put(pointList.get(0).getX());
			coordinateJson.put(pointList.get(0).getY());
		} else {
			geometryJson.put("type", "Polygon");
			
			for(OSMPoint point : pointList) {
				final JSONArray coordinatesJson = new JSONArray();
				coordinateJson.put(coordinatesJson);

				coordinatesJson.put(point.getX());
				coordinatesJson.put(point.getY());
			}
		}

		final JSONObject propertiesJson = new JSONObject();
		featureJson.put("properties", propertiesJson);
		for(final String key : properties.keySet()) {
			propertiesJson.put(key, properties.get(key));
		}

		return featureJson.toString(3);
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
		System.out.println(polygon.toGeoJson());

		System.out.println("=====================");
		final Polygon polygon2 = new Polygon(14);
		polygon2.addPoint(23.1, 23.1);
		System.out.println(polygon2.toGeoJson());
		
		System.out.println("=====================");
		final Polygon polygon3 = new Polygon(15);
		System.out.println(polygon3.toGeoJson());
	}
}

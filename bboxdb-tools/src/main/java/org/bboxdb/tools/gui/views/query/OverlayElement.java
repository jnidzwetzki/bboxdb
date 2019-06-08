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
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.viewer.GeoPosition;

public class OverlayElement {
	
	/**
	 * The polygon to draw
	 */
	private final GeoJsonPolygon polygon;
	
	/**
	 * The color to draw
	 */
	private final Color color;
	
	/**
	 * The points of the bounding box
	 */
	private final List<Point2D> boundingBoxPoints;

	public OverlayElement(final GeoJsonPolygon polygon, final Color color) {
		this.polygon = polygon;
		this.color = color;
		
		final Hyperrectangle bbox = polygon.getBoundingBox();
		boundingBoxPoints = new ArrayList<>();
		
		boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateLow(0), bbox.getCoordinateLow(1)));
		boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateHigh(0), bbox.getCoordinateLow(1)));
		boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateHigh(0), bbox.getCoordinateHigh(1)));
		boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateLow(0), bbox.getCoordinateHigh(1)));
		boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateLow(0), bbox.getCoordinateLow(1)));
		
	}

	/**
	 * Get the polygon to draw
	 * @return
	 */
	public GeoJsonPolygon getPolygon() {
		return polygon;
	}

	/**
	 * Get the color to use
	 * @return
	 */
	public Color getColor() {
		return color;
	}
	
	/**
	 * Get the points to draw on the GUI
	 * @param map
	 * @return
	 */
	public List<Point2D> getPointsToDrawOnGui(final JXMapViewer map) {
		final List<Point2D> polygonPoints = polygon.getPointList();
		return convertPointCoordinatesToGUICoordinates(map, polygonPoints);
	}
	
	/**
	 * Get the bounding box points to draw on the GUI
	 * @param map
	 * @return
	 */
	public List<Point2D> getBBoxPointsToDrawOnGui(final JXMapViewer map) {
		return convertPointCoordinatesToGUICoordinates(map, boundingBoxPoints);		
	}
	
	/**
	 * Convert a list with coordinate points to gui points
	 * @param map
	 * @param polygonPoints
	 * @return
	 */
	private List<Point2D> convertPointCoordinatesToGUICoordinates(final JXMapViewer map,
			final List<Point2D> polygonPoints) {
		
		final List<Point2D> elementPoints = new ArrayList<>();

		for(final Point2D point : polygonPoints) {
			final GeoPosition geoPosition = new GeoPosition(point.getX(), point.getY());
			final Point2D convertedPoint = map.getTileFactory().geoToPixel(geoPosition, map.getZoom());
			elementPoints.add(convertedPoint);
		}
		
		return elementPoints;
	}
}

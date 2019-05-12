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
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.painter.Painter;
import org.jxmapviewer.viewer.GeoPosition;


public class QueryResultOverlay implements Painter<JXMapViewer> {

	/**
	 * The data to draw
	 */
	private final Map<List<Point>, Color> dataToDraw;
	
	/**
	 * The transparency value
	 */
	private final static int TRANSPARENCY = 127;
	
	/**
	 * Our green
	 */
	public final static Color OUR_GREEN = new Color(Color.GREEN.getRed(), Color.GREEN.getGreen(), 
			Color.GREEN.getBlue(), TRANSPARENCY);
	
	/**
	 * Our red
	 */
	public final static Color OUR_RED = new Color(Color.RED.getRed(), Color.RED.getGreen(), 
			Color.RED.getBlue(), TRANSPARENCY);
	
	/**
	 * Our black
	 */
	public final static Color OUR_BLACK = Color.BLACK;

	public QueryResultOverlay(final Map<List<Point>, Color> dataToDraw) {
		this.dataToDraw = dataToDraw;
	}

	@Override
	public void paint(final Graphics2D g, final JXMapViewer map, final int width, final int height) {
		final Graphics2D graphicsContext = (Graphics2D) g.create();
		
		// convert from viewport to world bitmap
		final Rectangle rect = map.getViewportBounds();
		graphicsContext.translate(-rect.x, -rect.y);
		
		graphicsContext.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		
		final Color oldColor = graphicsContext.getColor();
		
		drawData(graphicsContext, map);
		
		graphicsContext.setColor(oldColor);
		graphicsContext.dispose();		
	}

	/**
	 * Draw the given data
	 * @param graphicsContext 
	 * @param map 
	 */
	private void drawData(final Graphics2D graphicsContext, final JXMapViewer map) {
		
		final Map<List<Point2D>, Color> pointsToDraw = convertThePointlist(map);
		
		for(final List<Point2D> pointList : pointsToDraw.keySet()) {
			
			if(pointList.size() == 1) {
				final Point2D thePoint = pointList.get(0);
				graphicsContext.drawOval((int) thePoint.getX(), (int) thePoint.getY(), 50, 50);
				continue;
			}
			
			final Point2D firstElement = pointList.get(0);
			final Point2D lastElement = pointList.get(pointList.size() - 1);
			
			if(firstElement.equals(lastElement)) {
				final Polygon polygon = new Polygon();
				
				for (final Point2D point : pointList) {
					polygon.addPoint((int) point.getX(), (int) point.getY()); 
				}
				
				graphicsContext.setColor(pointsToDraw.get(pointList));
				graphicsContext.drawPolygon(polygon);				
			} else {
				Point2D lastPoint = null;
				
				for (final Point2D point : pointList) {
					if(lastPoint != null) {
						graphicsContext.drawLine((int) lastPoint.getX(), (int) lastPoint.getY(), 
								(int) point.getX(), (int) point.getY());
					}
					lastPoint = point;
				}				
			}
		}
	}

	/**
	 * Convert the geo coordinates to real coordinates
	 * @param map
	 * @return
	 */
	private Map<List<Point2D>, Color> convertThePointlist(final JXMapViewer map) {
		
		final Map<List<Point2D>, Color> convertedPointList = new HashMap<>();
		
		for(final List<Point> element : dataToDraw.keySet()) {
			
			final List<Point2D> elementPoints = new ArrayList<>();
			convertedPointList.put(elementPoints, dataToDraw.get(element));
			
			for(final Point point : element) {
				final GeoPosition geoPosition = new GeoPosition(point.getX(), point.getY());
				final Point2D convertedPoint = map.getTileFactory().geoToPixel(geoPosition, map.getZoom());
				elementPoints.add(convertedPoint);
			}
		}
		
		return convertedPointList;
	}
}

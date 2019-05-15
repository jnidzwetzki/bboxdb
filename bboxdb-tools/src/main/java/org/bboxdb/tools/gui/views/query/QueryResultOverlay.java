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

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bboxdb.commons.Pair;
import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.painter.Painter;
import org.jxmapviewer.viewer.GeoPosition;


public class QueryResultOverlay implements Painter<JXMapViewer> {

	/**
	 * The data to draw
	 */
	private final Collection<Pair<List<Point2D>, Color>> dataToDraw;
	
	/**
	 * The transparency value
	 */
	private final static int TRANSPARENCY = 127;

	public QueryResultOverlay(final Collection<Pair<List<Point2D>, Color>> dataToDraw) {
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
		
		final Collection<Pair<List<Point2D>, Color>> pointsToDraw = convertThePointlist(map);
		
		for(final Pair<List<Point2D>, Color> entry : pointsToDraw) {
			
			final List<Point2D> pointList = entry.getElement1();
			final Color color = entry.getElement2();

			if(pointList.size() == 1) {
				final Point2D thePoint = pointList.get(0);
				graphicsContext.setColor(Color.BLUE);
				final int size = (int) (15 * (1.0 / map.getZoom()));
				graphicsContext.drawOval((int) thePoint.getX(), (int) thePoint.getY(), size, size);
				continue;
			}
			
			final Point2D firstElement = pointList.get(0);
			final Point2D lastElement = pointList.get(pointList.size() - 1);
			
			if(firstElement.equals(lastElement)) {
				final Polygon polygon = new Polygon();
				
				for (final Point2D point : pointList) {
					polygon.addPoint((int) point.getX(), (int) point.getY()); 
				}
				
				if(color.equals(Color.BLACK)) {
					graphicsContext.setColor(Color.BLACK);
					graphicsContext.drawPolygon(polygon);
				} else {
					final Color transparentColor = new Color(color.getRed(), color.getGreen(), 
							color.getBlue(), TRANSPARENCY);
					
					graphicsContext.setColor(transparentColor);
					graphicsContext.fillPolygon(polygon);				
				}
			} else {
				Point2D lastPoint = null;
				
				graphicsContext.setColor(color);
				graphicsContext.setStroke(new BasicStroke(2));

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
	private Collection<Pair<List<Point2D>, Color>> convertThePointlist(final JXMapViewer map) {
		
		final List<Pair<List<Point2D>, Color>> convertedPointList = new CopyOnWriteArrayList<>();
		
		for(final Pair<List<Point2D>, Color> element : dataToDraw) {
			
			final List<Point2D> elementPoints = new ArrayList<>();
			convertedPointList.add(new Pair<>(elementPoints, element.getElement2()));
			
			for(final Point2D point : element.getElement1()) {
				final GeoPosition geoPosition = new GeoPosition(point.getX(), point.getY());
				final Point2D convertedPoint = map.getTileFactory().geoToPixel(geoPosition, map.getZoom());
				elementPoints.add(convertedPoint);
			}	
		}
	
		return convertedPointList;
	}
}

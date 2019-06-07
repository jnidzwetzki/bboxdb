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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bboxdb.commons.Pair;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.painter.Painter;
import org.jxmapviewer.viewer.GeoPosition;


public class QueryResultOverlay implements Painter<JXMapViewer> {

	/**
	 * The data to draw
	 */
	private final Collection<Pair<GeoJsonPolygon, Color>> dataToDraw;
	
	/**
	 * The transparency value
	 */
	private final static int TRANSPARENCY = 127;
	
	/**
	 * The selection fill color
	 */
	private static Color SELECTION_FILL_COLOR = new Color(128, 192, 255, 128);
	
	/**
	 * The selection frame color
	 */
	private static Color SELECTION_FRAME_COLOR = new Color(0, 0, 255, 128);


	/**
	 * The selection adapter
	 */
	private QueryRangeSelectionAdapter selectionAdapter;
	
	/**
	 * Draw the bounding boxes of the objects
	 */
	private boolean drawBoundingBoxes = true;

	public QueryResultOverlay(final Collection<Pair<GeoJsonPolygon, Color>> dataToDraw, 
			final QueryRangeSelectionAdapter selectionAdapter) {
		this.dataToDraw = dataToDraw;
		this.selectionAdapter = selectionAdapter;
	}

	@Override
	public void paint(final Graphics2D g, final JXMapViewer map, final int width, final int height) {
		final Graphics2D graphicsContext = (Graphics2D) g.create();
		final Color oldColor = graphicsContext.getColor();
		
		final Optional<Rectangle> selection = selectionAdapter.getRectangle();
		if (selection.isPresent()) {
			g.setColor(SELECTION_FRAME_COLOR);
			g.draw(selection.get());
			g.setColor(SELECTION_FILL_COLOR);
			g.fill(selection.get());			
		}
		
		// convert from viewport to world bitmap
		final Rectangle rect = map.getViewportBounds();
		graphicsContext.translate(-rect.x, -rect.y);
		
		graphicsContext.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
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
		
		final Collection<Pair<List<Point2D>, Color>> pointsToDraw = buildPointlist(map);
		
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
	private Collection<Pair<List<Point2D>, Color>> buildPointlist(final JXMapViewer map) {
		
		final List<Pair<List<Point2D>, Color>> convertedPointList = new CopyOnWriteArrayList<>();
	
		
		for(final Pair<GeoJsonPolygon, Color> element : dataToDraw) {
	
			final GeoJsonPolygon polygon = element.getElement1();
			final List<Point2D> polygonPoints = polygon.getPointList();
			final List<Point2D> elementPoints = convertPointCoordinatesToGUICoordinates(map, polygonPoints);
			convertedPointList.add(new Pair<>(elementPoints, element.getElement2()));
			
			if(drawBoundingBoxes) {
				final Hyperrectangle bbox = polygon.getBoundingBox();
				final List<Point2D> boundingBoxPoints = new ArrayList<>();
				
				boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateLow(0), bbox.getCoordinateLow(1)));
				boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateHigh(0), bbox.getCoordinateLow(1)));
				boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateHigh(0), bbox.getCoordinateHigh(1)));
				boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateLow(0), bbox.getCoordinateHigh(1)));
				boundingBoxPoints.add(new Point2D.Double (bbox.getCoordinateLow(0), bbox.getCoordinateLow(1)));
				
				final List<Point2D> boxPoints = convertPointCoordinatesToGUICoordinates(map, boundingBoxPoints);
				convertedPointList.add(new Pair<>(boxPoints, Color.BLACK));
			}
		}
	
		return convertedPointList;
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
	
	/**
	 * Set the drawing of the bounding boxes
	 * @param drawBoundingBoxes
	 */
	public void setDrawBoundingBoxes(final boolean drawBoundingBoxes) {
		this.drawBoundingBoxes = drawBoundingBoxes;
	}

}

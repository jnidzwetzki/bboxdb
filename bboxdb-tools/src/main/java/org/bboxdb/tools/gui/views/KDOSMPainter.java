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
package org.bboxdb.tools.gui.views;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.geom.Point2D;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.tools.gui.GuiModel;
import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.painter.Painter;
import org.jxmapviewer.viewer.GeoPosition;

public class KDOSMPainter implements Painter<JXMapViewer> {
	
	/**
	 * Max latitude
	 */
	protected final static double MAX_LAT = 85;
	
	/**
	 * Min latitude
	 */
	protected final static double MIN_LAT = -85;
	
	/**
	 * Max longitude
	 */
	protected final static double MAX_LONG = 180;
	
	/**
	 * Min longitude
	 */
	protected final static double MIN_LONG = -180;

	/**
	 * The area covering box
	 */
	protected final BoundingBox coverBox = new BoundingBox((double) MIN_LAT, (double) MAX_LAT, (double) MIN_LONG, (double) MAX_LONG);

	/**
	 * The gui model
	 */
	protected GuiModel guiModel;

	public KDOSMPainter(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}

	@Override
	public void paint(final Graphics2D g, final JXMapViewer map, final int w, final int h) {
		final Graphics2D graphicsContext = (Graphics2D) g.create();
		
		// convert from viewport to world bitmap
		Rectangle rect = map.getViewportBounds();
		graphicsContext.translate(-rect.x, -rect.y);
		
		graphicsContext.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		
		graphicsContext.setColor(Color.BLACK);
		graphicsContext.setStroke(new BasicStroke(2));
		
		drawKDTree(graphicsContext, map);
		graphicsContext.dispose();
	}

	/**
	 * Draw the KD Tree
	 * @param graphicsContext
	 * @param map 
	 */
	protected void drawKDTree(final Graphics2D graphicsContext, final JXMapViewer map) {

		// No distribution group is selected
		if(guiModel.getTreeAdapter() == null || guiModel.getTreeAdapter().getRootNode() == null) {
			return;
		}
		
		final DistributionRegion distributionRegion = guiModel.getTreeAdapter().getRootNode();
		final BoundingBox bbox = distributionRegion.getConveringBox();

		if(bbox.getDimension() != 2) {
			System.err.println("Unable to print dimensions: " + bbox.getDimension());
			return;
		}
		
		drawBoundingBox(graphicsContext, map, distributionRegion);
	}

	/**
	 * Draw a bounding box on the screen
	 * 
	 */
	protected void drawBoundingBox(final Graphics2D graphicsContext, final JXMapViewer map, final DistributionRegion distributionRegion) {
		final BoundingBox bbox = distributionRegion.getConveringBox();

		final BoundingBox paintBox = bbox.getIntersection(coverBox);
				
		final GeoPosition bl = new GeoPosition(paintBox.getCoordinateLow(0), paintBox.getCoordinateLow(1));
		final GeoPosition br = new GeoPosition(paintBox.getCoordinateLow(0), paintBox.getCoordinateHigh(1));
		final GeoPosition ul = new GeoPosition(paintBox.getCoordinateHigh(0), paintBox.getCoordinateLow(1));
		final GeoPosition ur = new GeoPosition(paintBox.getCoordinateHigh(0), paintBox.getCoordinateHigh(1));

		drawLine(graphicsContext, bl, br, map);
		drawLine(graphicsContext, br, ur, map);
		drawLine(graphicsContext, ur, ul, map);
		drawLine(graphicsContext, ul, bl, map);
	
		if(distributionRegion.getLeftChild() != null) {
			drawBoundingBox(graphicsContext, map, distributionRegion.getLeftChild());
		}
		
		if(distributionRegion.getRightChild() != null) {
			drawBoundingBox(graphicsContext, map, distributionRegion.getRightChild());
		}
	}

	/**
	 * Translate and draw a line
	 * @param graphicsContext
	 * @param begin
	 * @param end
	 * @param map
	 */
	protected void drawLine(final Graphics2D graphicsContext, final GeoPosition begin, 
			final GeoPosition end, final JXMapViewer map) {
	
		final Point2D beginPoint = map.getTileFactory().geoToPixel(begin, map.getZoom());
		final Point2D endPoint = map.getTileFactory().geoToPixel(end, map.getZoom());
		graphicsContext.drawLine((int) beginPoint.getX(), (int) beginPoint.getY(), (int) endPoint.getX(), (int) endPoint.getY());
	}

}

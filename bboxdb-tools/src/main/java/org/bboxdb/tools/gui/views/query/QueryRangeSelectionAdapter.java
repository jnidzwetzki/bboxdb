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

import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.Point2D;
import java.util.Collection;
import java.util.Optional;

import javax.swing.SwingUtilities;

import org.bboxdb.tools.gui.GuiModel;
import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.viewer.GeoPosition;
import org.jxmapviewer.viewer.TileFactory;

public class QueryRangeSelectionAdapter extends MouseAdapter {

	/**
	 * Is a dragging action active?
	 */
	private boolean dragging;
	
	/**
	 * The viewer
	 */
	private final JXMapViewer viewer;

	/**
	 * The start position
	 */
	private Point2D startPos = new Point2D.Double();
	
	/**
	 * The stop position
	 */
	private Point2D stopPos = new Point2D.Double();
	
	/**
	 * The global data to draw
	 */
	private final Collection<OverlayElement> dataToDraw;
	
	/**
	 * The global gui model
	 */
	private final GuiModel guiModel;

	public QueryRangeSelectionAdapter(final Collection<OverlayElement> dataToDraw, 
			final GuiModel guiModel, final JXMapViewer viewer) {
		
		this.dataToDraw = dataToDraw;
		this.guiModel = guiModel;
		this.viewer = viewer;
	}

	@Override
	public void mousePressed(final MouseEvent e) {

		if (!SwingUtilities.isRightMouseButton(e)) {
			return;
		}

		startPos.setLocation(e.getX(), e.getY());
		dragging = true;
	}

	/**
	 * pixel coordinates are world coordinates
	 * 
	 * @param e
	 * @return
	 */
	private Point2D getRealPos(Point2D point) {
		final Point2D pos = new Point2D.Double();
		final Rectangle viewport = viewer.getViewportBounds();
		pos.setLocation(point.getX() + viewport.getX(), point.getY() + viewport.getY());
		return pos;
	}

	@Override
	public void mouseDragged(final MouseEvent e) {
		updateStatusBar(e);
				
		if (! dragging) {
			return;
		}

		stopPos.setLocation(e.getX(), e.getY());

		viewer.repaint();
	}
	
	@Override
	public void mouseMoved(final MouseEvent e) {
		updateStatusBar(e);
	}

	/**
	 * Update the status bar
	 * @param e
	 */
	private void updateStatusBar(final MouseEvent e) {
		final Point2D mousePos = new Point2D.Double(e.getX(), e.getY());
		final Point2D realPos = getRealPos(mousePos);
		final TileFactory tileFactory = viewer.getTileFactory();
		final GeoPosition pos = tileFactory.pixelToGeo(realPos, viewer.getZoom());

		guiModel.setStatusText("Longitude " + pos.getLongitude() + " Latitude " + pos.getLatitude());
	}

	@Override
	public void mouseReleased(final MouseEvent e) {

		if (!dragging) {
			return;
		}

		if (!SwingUtilities.isRightMouseButton(e)) {
			return;
		}

		viewer.repaint();
		dragging = false;

		final TileFactory tileFactory = viewer.getTileFactory();
		final Point2D realStartPos = getRealPos(startPos);
		final GeoPosition beginPos = tileFactory.pixelToGeo(realStartPos, viewer.getZoom());
		final Point2D realEndPos = getRealPos(stopPos);
		final GeoPosition endPos = tileFactory.pixelToGeo(realEndPos, viewer.getZoom());

		final QueryWindow queryWindow = new QueryWindow(guiModel, dataToDraw, () -> {
			viewer.repaint();
		});
		
		final double minLong = Math.min(beginPos.getLongitude(), endPos.getLongitude());
		final double maxLong = Math.max(beginPos.getLongitude(), endPos.getLongitude());
		final double minLat = Math.min(beginPos.getLatitude(), endPos.getLatitude());
		final double maxLat = Math.max(beginPos.getLatitude(), endPos.getLatitude());
				
		queryWindow.setSelectedLongBegin(Double.toString(minLong));
		queryWindow.setSelectedLongEnd(Double.toString(maxLong));
		queryWindow.setSelectedLatBegin(Double.toString(minLat));
		queryWindow.setSelectedLatEnd(Double.toString(maxLat));

		queryWindow.show();		
	}

	/**
	 * Get the overlay for drawing
	 * @return
	 */
	public Optional<Rectangle> getRectangle() {
		
		if (dragging) {
			int x1 = (int) Math.min(startPos.getX(), stopPos.getX());
			int y1 = (int) Math.min(startPos.getY(), stopPos.getY());
			int x2 = (int) Math.max(startPos.getX(), stopPos.getX());
			int y2 = (int) Math.max(startPos.getY(), stopPos.getY());

			return Optional.of(new Rectangle(x1, y1, x2 - x1, y2 - y1));
		}

		return Optional.empty();
	}
}

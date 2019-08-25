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
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.painter.Painter;


public class ElementOverlayPainter implements Painter<JXMapViewer> {

	/**
	 * The data to draw
	 */
	private final Collection<ResultTuple> tupleToDraw;

	/**
	 * The rendered elements
	 */
	private final List<OverlayElement> renderedElements = new ArrayList<OverlayElement>();

	/**
	 * Data valid for
	 */
	private Rectangle validForRectangle;
	
	/**
	 * Data valid for
	 */
	private long validForElements;

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
	
	/**
	 * The callbacks for a changed list
	 */
	private final List<Consumer<List<OverlayElement>>> callbacks;

	public ElementOverlayPainter(final List<ResultTuple> tupleToDraw, 
			final QueryRangeSelectionAdapter selectionAdapter) {
		
		this.tupleToDraw = tupleToDraw;
		this.selectionAdapter = selectionAdapter;
		this.callbacks = new CopyOnWriteArrayList<>();
	}

	@Override
	public void paint(final Graphics2D g, final JXMapViewer map, final int width, final int height) {
		final Graphics2D graphicsContext = (Graphics2D) g.create();
		final Color oldColor = graphicsContext.getColor();
		
		// Paint mouse selection rectangle (if present)
		final Optional<Rectangle> selection = selectionAdapter.getRectangle();
		if (selection.isPresent()) {
			g.setColor(SELECTION_FRAME_COLOR);
			g.draw(selection.get());
			g.setColor(SELECTION_FILL_COLOR);
			g.fill(selection.get());			
		}
		
		// Paint overlay elements
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
		
		final Rectangle viewBounds = map.getViewportBounds();
		
		if(hasDataChanged(viewBounds)) {
			
			validForRectangle = viewBounds;
			validForElements = tupleToDraw.size();
			
			renderedElements.clear();
			
			for(final ResultTuple tuple: tupleToDraw) {
				
				final int tuples  = tuple.getNumberOfTuples();
				
				for(int i = 0; i < tuples; i++) {
					final OverlayElement element = tuple.getOverlayForTuple(i);
					element.updatePosition(map);
					
					final Rectangle boundingBox = element.getBBoxToDrawOnGui();
					
					// Skip rendering of invisible elements
					if(! viewBounds.intersects(boundingBox)) {
						continue;
					}
					
					renderedElements.add(element);
				}
			}
			
			// Notify callbacks
			final List<OverlayElement> elementsForCallback = Collections.unmodifiableList(renderedElements);
			
			for(final Consumer<List<OverlayElement>> callback : callbacks) {
				callback.accept(elementsForCallback);
			}
		}
		
		for(final OverlayElement element : renderedElements) {
			element.drawPointListOnGui(graphicsContext, map);
			
			if(drawBoundingBoxes) {
				final Rectangle boundingBox = element.getBBoxToDrawOnGui();
				graphicsContext.setColor(Color.BLACK);
				graphicsContext.draw(boundingBox);
			}
		}
	}

	/**
	 * Was the map moved?
	 * @param viewBounds
	 * @return
	 */
	private boolean hasDataChanged(final Rectangle viewBounds) {
		
		if(validForRectangle == null || validForElements == 0) {
			return true;
		}
		
		if(! validForRectangle.equals(viewBounds)) {
			return true;
		}
		
		if(validForElements != tupleToDraw.size()) {
			return true;
		}
		
		return false;
	}
	
	
	
	/**
	 * Set the drawing of the bounding boxes
	 * @param drawBoundingBoxes
	 */
	public void setDrawBoundingBoxes(final boolean drawBoundingBoxes) {
		this.drawBoundingBoxes = drawBoundingBoxes;
	}
	
	/**
	 * Register a callback
	 */
	public void registerCallback(final Consumer<List<OverlayElement>> callback) {
		callbacks.add(callback);
	}
	
}

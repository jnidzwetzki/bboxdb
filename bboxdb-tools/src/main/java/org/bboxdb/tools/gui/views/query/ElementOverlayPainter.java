/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
import java.awt.EventQueue;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import javax.swing.SwingUtilities;

import org.bboxdb.storage.entity.EntityIdentifier;
import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.painter.Painter;


public class ElementOverlayPainter implements Painter<JXMapViewer> {

	/**
	 * The data to draw
	 */
	private final Collection<OverlayElementGroup> tupleToDraw;

	/**
	 * The rendered elements
	 */
	private final List<OverlayElement> renderedElements;

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
	private final QueryRangeSelectionAdapter selectionAdapter;
	
	/**
	 * Draw the bounding boxes of the objects
	 */
	private ElementPaintMode paintMode = ElementPaintMode.ALL;
	
	/**
	 * The callbacks for a changed list
	 */
	private final List<Consumer<List<OverlayElement>>> callbacks;
	
	/**
	 * The map viewer
	 */
	private final JXMapViewer mapViewer;
	

	public ElementOverlayPainter(final QueryRangeSelectionAdapter selectionAdapter, 
			final JXMapViewer mapViewer) {
		
		this.selectionAdapter = selectionAdapter;
		this.mapViewer = mapViewer;
		this.renderedElements = new ArrayList<>();
		this.tupleToDraw = new CopyOnWriteArrayList<>();
		this.callbacks = new CopyOnWriteArrayList<>();
	}
	
	/**
	 * Repaint the given area
	 * @param mapViewer
	 * @param bbox
	 */
	public boolean markRegionAsDirty(final OverlayElementGroup overlayElementGroup) {
		
		if(overlayElementGroup.getNumberOfOverlays() == 0) {
			return false;
		}
		
		final Rectangle bbox = new Rectangle(overlayElementGroup.getOverlay(0).getBBoxToDrawOnGui());
		
		for(final OverlayElement element : overlayElementGroup) {
			bbox.add(element.getDirtyPixel());
		}
		
		final Rectangle rect = mapViewer.getViewportBounds();
		
		bbox.translate((int) -rect.getX(), (int) -rect.getY());
		
		if(EventQueue.isDispatchThread()) {
			mapViewer.repaint(bbox);
		} else {
			SwingUtilities.invokeLater(() -> mapViewer.repaint(bbox));
		}

		return true;
	}
	
	/**
	 * Repaint all data
	 */
	public void repaintAll() {
		if(EventQueue.isDispatchThread()) {
			mapViewer.repaint();
		} else {
			SwingUtilities.invokeLater(() -> mapViewer.repaint());
		}
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
			
			for(final OverlayElementGroup tuple: tupleToDraw) {
				for(final OverlayElement element : tuple) {
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
		
		final Set<EntityIdentifier> alreadyRenderedElements = new HashSet<>();
		
		// Render selected elements first (prevent duplicate selected overlays)
		for(final OverlayElement element : renderedElements) {
			if(! element.isSelected()) {
				continue;
			}
			
			final EntityIdentifier identifier = element.getEntityIdentifier();
			renderElement(graphicsContext, map, alreadyRenderedElements, element, identifier);	
		}
		
		// Render remaining elements
		for(final OverlayElement element : renderedElements) {
			final EntityIdentifier identifier = element.getEntityIdentifier();
			
			renderElement(graphicsContext, map, alreadyRenderedElements, element, identifier);
		}
	}
	
	/**
	 * Render the selected element
	 * @param graphicsContext
	 * @param map
	 * @param alreadyRenderedElements
	 * @param element
	 * @param identifier
	 */
	private void renderElement(final Graphics2D graphicsContext, final JXMapViewer map,
			final Set<EntityIdentifier> alreadyRenderedElements, final OverlayElement element,
			final EntityIdentifier identifier) {
		
		if(paintMode == ElementPaintMode.ALL || paintMode == ElementPaintMode.GEOMETRY_ONLY) {
			final boolean drawReally = alreadyRenderedElements.add(identifier);
			element.drawOnGui(graphicsContext, map, drawReally);
		}

		if(paintMode == ElementPaintMode.ALL || paintMode == ElementPaintMode.BOUNDING_BOXES_ONLY) {
			final Rectangle boundingBox = element.getBBoxToDrawOnGui();
			graphicsContext.setColor(Color.BLACK);
			graphicsContext.draw(boundingBox);
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
	 * Set the paint mode
	 * @param paintMode
	 */
	public void setPaintMode(final ElementPaintMode paintMode) {
		this.paintMode = paintMode;
	}
	
	/**
	 * Register a callback
	 */
	public void registerCallback(final Consumer<List<OverlayElement>> callback) {
		callbacks.add(callback);
	}
	
	/**
	 * Add an element to draw
	 * @param element
	 * @param refreshGUI 
	 */
	public void addElementToDraw(final OverlayElementGroup element, final boolean refreshGUI) {
		tupleToDraw.add(element);
		repaintElement(element, true);
		setDirty();
		
		if(refreshGUI) {
			mapViewer.repaint();
		}
	}
	
	/**
	 * Add an element to draw
	 * @param element
	 */
	public void addElementToDrawBulk(final Collection<OverlayElementGroup> elements) {
		tupleToDraw.addAll(elements);
		setDirty();
		mapViewer.repaint();
	}

	/**
	 * Repaint the given element
	 * @param element
	 * @param recalulatePosition
	 */
	private void repaintElement(final OverlayElementGroup element, final boolean recalulatePosition) {
		
		for(final OverlayElement overlayElement : element) {
			
			if(recalulatePosition) {
				overlayElement.updatePosition(mapViewer);
			}
		}
		
		markRegionAsDirty(element);
	}
	
	/**
	 * Remove an element to draw
	 * @param element
	 * @param refreshGUI 
	 */
	public void removeElementToDraw(final OverlayElementGroup element, final boolean refreshGUI) {
		tupleToDraw.remove(element);
		setDirty();
		
		if(refreshGUI) {
			repaintElement(element, false);
		}
	}

	/**
	 * Set dirty
	 */
	private void setDirty() {
		validForRectangle = null;
	}
	
	/**
	 * Clear all data to draw
	 */
	public void clearAllElements() {
		tupleToDraw.clear();
		setDirty();
		mapViewer.repaint();
	}
	
	/**
	 * Get the map viewer
	 * @return
	 */
	public JXMapViewer getMapViewer() {
		return mapViewer;
	}
	
}

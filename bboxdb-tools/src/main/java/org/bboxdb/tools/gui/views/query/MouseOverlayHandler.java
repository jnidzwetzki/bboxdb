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
import java.util.Collection;

import org.jxmapviewer.JXMapViewer;

public class MouseOverlayHandler extends MouseAdapter {
	
	/**
	 * The map viewer
	 */
	private final JXMapViewer mapViewer;
	
	/**
	 * The overlay painter
	 */
	private final ElementOverlayPainter painter;

	MouseOverlayHandler(JXMapViewer mapViewer, final ElementOverlayPainter painter) {
		this.mapViewer = mapViewer;
		this.painter = painter;
	}

	@Override
	public void mouseMoved(final MouseEvent e) {
		
		final Collection<OverlayElement> renderedElements = painter.getRenderedElements();
		
		final Rectangle rect = mapViewer.getViewportBounds();

		final Rectangle mousePos = new Rectangle((int) (e.getX() + rect.getX()), 
				(int) (e.getY() + rect.getY()), 1, 1);
		
		for(final OverlayElement element : renderedElements) {
			final Rectangle bbox = element.getBBoxToDrawOnGui();
			
			if(bbox.intersects(mousePos)) {
				
				if(element.isHighlighted()) {
					continue;
				}
				
				element.setHighlight(true);
				repaintElement(rect, bbox);
			} else {
				if(! element.isHighlighted()) {
					continue;
				}
				
				element.setHighlight(false);
				repaintElement(rect, bbox);
			}
		}
		
	}

	/**
	 * Repaint the given area
	 * @param rect
	 * @param bbox
	 */
	private void repaintElement(final Rectangle rect, final Rectangle bbox) {
		final Rectangle translatedBBox = new Rectangle(bbox);
		translatedBBox.translate((int) -rect.getX(), (int) -rect.getY());
		mapViewer.repaint(translatedBBox);
	}
}
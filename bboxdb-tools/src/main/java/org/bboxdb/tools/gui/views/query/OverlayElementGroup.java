/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import java.awt.EventQueue;
import java.awt.Rectangle;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

import javax.swing.SwingUtilities;

import org.jxmapviewer.JXMapViewer;

public class OverlayElementGroup implements Iterable<OverlayElement> {

	/**
	 * The elements of the result
	 */
	public final List<OverlayElement> elements;
	
	/**
	 * Is the group selected
	 */
	private volatile boolean selected;

	public OverlayElementGroup(final List<OverlayElement> elements) {
		this.elements = elements;
		this.selected = false;
		
		for(final OverlayElement overlayElement : elements) {
			overlayElement.setOverlayElementGroup(this);
		}
	}
	
	/**
	 * Repaint the given area
	 * @param mapViewer
	 * @param bbox
	 */
	public boolean repaintElement(final JXMapViewer mapViewer) {
		
		if(elements.isEmpty()) {
			return false;
		}
		
		final Rectangle bbox = new Rectangle(getOverlay(0).getBBoxToDrawOnGui());
		
		for(final OverlayElement element : elements) {
			bbox.add(element.getDirtyPixel());
		}
		
		final Rectangle rect = mapViewer.getViewportBounds();
		
		bbox.translate((int) -rect.getX(), (int) -rect.getY());

		try {
			if(EventQueue.isDispatchThread()) {
				mapViewer.repaint(bbox);
			} else {
				SwingUtilities.invokeAndWait(() -> mapViewer.repaint(bbox));
			}
		} catch (InvocationTargetException | InterruptedException e) {
			e.printStackTrace();
		}
		
		return true;
	}
	
	/**
	 * Get the number of overlays contained in this group
	 * @return
	 */
	public int getNumberOfOverlays() {
		return elements.size();
	}
	
	/**
	 * Get the overlay with the specific ID
	 * @param id
	 * @return
	 */
	public OverlayElement getOverlay(final int id) {
		return elements.get(id);
	}
	
	/**
	 * Is the group selected?
	 * @return
	 */
	public boolean isSelected() {
		return selected;
	}
	
	/**
	 * Set the selected state of the group
	 * @param selected
	 */
	public void setSelected(final boolean selected) {
		this.selected = selected;
	}

	@Override
	public Iterator<OverlayElement> iterator() {
		return elements.iterator();
	}

	@Override
	public String toString() {
		return "OverlayElementGroup [elements=" + elements + ", selected=" + selected + "]";
	}
	
}

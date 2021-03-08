/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.swing.JToolTip;

import org.jxmapviewer.JXMapViewer;

public class MouseOverlayHandler extends MouseAdapter {
	
	/**
	 * The map viewer
	 */
	private final ElementOverlayPainter overlayPainter;
	
	/**
	 * The overlay painter
	 */
	private Collection<OverlayElement> renderedElements;
	
	/**
	 * The highlighted elements
	 */
	private final List<OverlayElement> highlightedElements = new ArrayList<>();

	/**
	 * The tooltip
	 */
	private final JToolTip toolTip;

	MouseOverlayHandler(final ElementOverlayPainter painter, final JToolTip toolTip) {
		this.overlayPainter = painter;
		this.toolTip = toolTip;
	}
	
	/**
	 * Update the rendered elements
	 * 
	 * @param renderedElements
	 */
	public void setRenderedElements(final Collection<OverlayElement> renderedElements) {
		this.renderedElements = renderedElements;
	}

	@Override
	public void mouseMoved(final MouseEvent e) {

		if(renderedElements == null) {
			return;
		}
		
		final JXMapViewer mapViewer = overlayPainter.getMapViewer();
		
		final Rectangle rect = mapViewer.getViewportBounds();
		final Point mousePosPoint = new Point((int) (e.getX() + rect.getX()), 
				(int) (e.getY() + rect.getY()));

		final Rectangle mousePos = new Rectangle(mousePosPoint);
		mousePos.setSize(1, 1);
		
		for(final OverlayElement element : renderedElements) {
			final Shape shape = element.getLastRenderedShape();
			
			if(shape != null && shape.intersects(mousePos)) {
				if(element.isSelected()) {
					continue;
				}
				
				element.setSelected(true);
				highlightedElements.add(element);
				overlayPainter.markRegionAsDirty(element.getOverlayElementGroup());
			}
		}
		
		for (final Iterator<OverlayElement> iterator = highlightedElements.iterator(); iterator.hasNext();) {
			final OverlayElement element = (OverlayElement) iterator.next();
			final Shape shape = element.getLastRenderedShape();

			if((shape != null && ! shape.intersects(mousePos)) || ! renderedElements.contains(element)) {
				iterator.remove();
				element.setSelected(false);
				overlayPainter.markRegionAsDirty(element.getOverlayElementGroup());
			}
		}
		
		if(! highlightedElements.isEmpty()) {
			final StringBuilder sb = new StringBuilder();
			
			final Set<String> knownElements = new HashSet<>();
			
			for(final OverlayElement element : highlightedElements) {
				if(! element.isSelected()) {
					continue;
				}
				
				final OverlayElementGroup group = element.getOverlayElementGroup();
				
				for(int i = 0; i < group.getNumberOfOverlays(); i++) {
					final OverlayElement overlayElement = group.getOverlay(i);
					final String tooltipText = overlayElement.getTooltipText();
					
					if(knownElements.contains(tooltipText)) {
						continue;
					}
					
					knownElements.add(tooltipText);

					if(i != 0) {
						sb.append("<hr>");
					}
					sb.append("%%FONT_START%%");
					sb.append(tooltipText);
					sb.append("</font>");
				}
			}
			
			final int lines = sb.toString().split("\\<br\\>").length;
			System.out.println("==> " + lines);
			
		
			final StringBuilder htmlStringBuilder = new StringBuilder("<html>");
			htmlStringBuilder.append(sb);
			htmlStringBuilder.append("</html>");
			
			String htmlString = htmlStringBuilder.toString();

			if(lines > 45) {
				htmlString = htmlString.replace("%%FONT_START%%", "<font size=\"1\">");
			} else if(lines > 25) {
				htmlString = htmlString.replace("%%FONT_START%%", "<font size=\"2\">");
			} else {
				htmlString = htmlString.replace("%%FONT_START%%", "<font size=\"3\">");
			}
						
			toolTip.setTipText(htmlString);
			toolTip.setLocation(new Point(e.getX(), e.getY()));
			toolTip.setVisible(true);
		} else {
			toolTip.setVisible(false);
		}
	}
}
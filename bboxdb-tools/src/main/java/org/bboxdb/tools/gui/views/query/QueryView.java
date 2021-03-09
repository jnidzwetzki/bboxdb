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

import java.awt.BorderLayout;
import java.awt.Point;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JToolTip;

import org.bboxdb.tools.gui.GuiModel;
import org.bboxdb.tools.gui.util.MapViewerFactory;
import org.bboxdb.tools.gui.views.View;
import org.jxmapviewer.JXMapViewer;

public class QueryView implements View {

	/**
	 * The GUI model
	 */
	private final GuiModel guiModel;
	
	/**
	 * The map viewer
	 */
	private JXMapViewer mapViewer;
	
	/**
	 * The details screen
	 */
	private final ResultDetailsWindow resultDetailsWindow;
	
	/**
	 * The running background threads
	 */
	private final List<Thread> backgroundThreads;
	
	/**
	 * The element overlay painter
	 */
	private ElementOverlayPainter painter;

	public QueryView(final GuiModel guiModel) {
		this.guiModel = guiModel;
		this.resultDetailsWindow = new ResultDetailsWindow();
		this.backgroundThreads = new ArrayList<>();
	}
	
	@Override
	public JComponent getJPanel() {
		mapViewer = MapViewerFactory.createMapViewer();
		resultDetailsWindow.setMapViewer(mapViewer);
		
        final QueryRangeSelectionAdapter selectionAdapter = new QueryRangeSelectionAdapter( 
        		guiModel, mapViewer, backgroundThreads);
        	
		painter = new ElementOverlayPainter(selectionAdapter, mapViewer);
		mapViewer.setOverlayPainter(painter);
		selectionAdapter.setElementOverlayPainter(painter);
	
        mapViewer.addMouseListener(selectionAdapter);
        mapViewer.addMouseMotionListener(selectionAdapter);
		
	     // Prepare tooltip (Rendered by the MouseOverlayHandler)
        final JToolTip tooltip = new JToolTip() {
        
			private static final long serialVersionUID = -2806858564323423227L;

			@Override
        	public void setLocation(int x, int y) {
        		// Ignore component layout requests
        	}
        	
        	@Override
        	public void setLocation(Point p) {
        		super.setLocation((int) p.getX(), (int) p.getY());
        	}
        };
        tooltip.setComponent(mapViewer);
        mapViewer.add(tooltip);
        
		final MouseOverlayHandler mouseOverlayHandler = new MouseOverlayHandler(painter, tooltip);
		mapViewer.addMouseMotionListener(mouseOverlayHandler);
		
		painter.registerCallback((e) -> mouseOverlayHandler.setRenderedElements(e));
		painter.registerCallback((e) -> resultDetailsWindow.setRenderedElements(e));

		final JPanel mainPanel = new JPanel();
		
		mainPanel.setLayout(new BorderLayout());
		mainPanel.add(mapViewer, BorderLayout.CENTER);

		final JPanel buttonPanel = new JPanel();
		mainPanel.add(buttonPanel, BorderLayout.SOUTH);
		
		final JButton zoomInButton = MapViewerFactory.getZoomInButton(mapViewer);
		buttonPanel.add(zoomInButton);

		final JButton zoomOutButton = MapViewerFactory.getZoomOutButton(mapViewer);		
		buttonPanel.add(zoomOutButton);
		
		final JButton showWorldButton = MapViewerFactory.getShowWorldButton(mapViewer);
		buttonPanel.add(showWorldButton);
		
		final JButton showLAButton = MapViewerFactory.getShowLAButton(mapViewer);
		buttonPanel.add(showLAButton);
		
		final JButton showBerlinButton = MapViewerFactory.getShowBerlinButton(mapViewer);
		buttonPanel.add(showBerlinButton);
		
		final JButton showSydneyButton = MapViewerFactory.getShowSydneyButton(mapViewer);
		buttonPanel.add(showSydneyButton);
		
		final JButton clearButton = getClearButton();
		buttonPanel.add(clearButton);
		
		final JButton resultDetails = getResultDetailsButton();
		buttonPanel.add(resultDetails);
		
		final JComponent bboxCheckbox = getDrawModeComponent(painter);
		buttonPanel.add(bboxCheckbox);

		return mainPanel;	
	}

	private JButton getResultDetailsButton() {
		final JButton queryButton = new JButton("Show result details");
		
		queryButton.addActionListener(l -> {
			resultDetailsWindow.show();
		});
		
		return queryButton;
	}

	/**
	 * Get the rendering mode component
	 */
	private JComponent getDrawModeComponent(final ElementOverlayPainter painter) {
		final ElementPaintMode[] valuesAsEnum = ElementPaintMode.values();
		
		final String[] valuesAsString = Arrays.asList(valuesAsEnum)
				.stream()
				.map(e -> e.getStringValue())
				.toArray(String[]::new);
		
		final JComboBox<String> dropdown = new JComboBox<>(valuesAsString);
		dropdown.setSelectedIndex(2);
		
		dropdown.addActionListener((a) -> {
			final ElementPaintMode mode = valuesAsEnum[dropdown.getSelectedIndex()];
			painter.setPaintMode(mode);
			mapViewer.repaint();
		});
		
		return dropdown;
	}

	/**
	 * Get the clear map button
	 * @return
	 */
	private JButton getClearButton() {
		final JButton clearButton = new JButton("Clear map");
		
		clearButton.addActionListener(l -> {
			backgroundThreads.forEach(t -> t.interrupt());
			backgroundThreads.clear();
			
			if(painter != null) {
				painter.clearAllElements();
			}
			
			mapViewer.repaint();
		});
		
		return clearButton;
	}

	@Override
	public boolean isGroupSelectionNeeded() {
		return false;
	}
}

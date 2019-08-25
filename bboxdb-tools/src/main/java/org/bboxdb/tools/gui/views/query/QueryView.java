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

import java.awt.BorderLayout;
import java.awt.Point;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.swing.JButton;
import javax.swing.JCheckBox;
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
	 * The data to draw
	 */
	private final List<ResultTuple> tupleToDraw;
	
	/**
	 * The details screen
	 */
	private final ResultDetailsWindow resultDetailsWindow;

	public QueryView(final GuiModel guiModel) {
		this.guiModel = guiModel;
		this.tupleToDraw = new CopyOnWriteArrayList<>();
		this.resultDetailsWindow = new ResultDetailsWindow();
	}
	
	@Override
	public JComponent getJPanel() {
		mapViewer = MapViewerFactory.createMapViewer();
		resultDetailsWindow.setMapViewer(mapViewer);
		
        final QueryRangeSelectionAdapter selectionAdapter = new QueryRangeSelectionAdapter(tupleToDraw, 
        		guiModel, mapViewer);
        
        mapViewer.addMouseListener(selectionAdapter);
        mapViewer.addMouseMotionListener(selectionAdapter);
		
		final ElementOverlayPainter painter = new ElementOverlayPainter(tupleToDraw, selectionAdapter);
		mapViewer.setOverlayPainter(painter);
		
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
        
		final MouseOverlayHandler mouseOverlayHandler = new MouseOverlayHandler(mapViewer, tooltip);
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
		
		final JButton showWolrdButton = MapViewerFactory.getShowWorldButton(mapViewer);
		buttonPanel.add(showWolrdButton);
		
		final JButton showHagenButton = MapViewerFactory.getShowHagenButton(mapViewer);
		buttonPanel.add(showHagenButton);
		
		final JButton showBerlinButton = MapViewerFactory.getShowBerlinButton(mapViewer);
		buttonPanel.add(showBerlinButton);
		
		final JButton queryButton = getQueryButton();
		buttonPanel.add(queryButton);
		
		final JButton clearButton = getClearButton();
		buttonPanel.add(clearButton);
		
		final JButton resultDetails = getResultDetailsButton();
		buttonPanel.add(resultDetails);
		
		final JCheckBox bboxCheckbox = getBBoxCheckbox(painter);
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
	 * Get the bounding box checkbox
	 */
	private JCheckBox getBBoxCheckbox(final ElementOverlayPainter painter) {
		final JCheckBox bboxCheckbox = new JCheckBox("Show Bounding Boxes");
		bboxCheckbox.setSelected(true);
		
		bboxCheckbox.addActionListener((a) -> {
			painter.setDrawBoundingBoxes(bboxCheckbox.isSelected());
			mapViewer.repaint();
		});
		
		return bboxCheckbox;
	}

	/**
	 * Get the query button
	 * @return
	 */
	private JButton getQueryButton() {
		final JButton queryButton = new JButton("Execute query");
		
		queryButton.addActionListener(l -> {
			final QueryWindow queryWindow = new QueryWindow(guiModel, tupleToDraw, () -> {
				mapViewer.repaint();
			});

			queryWindow.show();
		});
		
		return queryButton;
	}

	/**
	 * Get the clear map button
	 * @return
	 */
	private JButton getClearButton() {
		final JButton clearButton = new JButton("Clear map");
		
		clearButton.addActionListener(l -> {
			tupleToDraw.clear();
			mapViewer.repaint();
		});
		
		return clearButton;
	}

	@Override
	public boolean isGroupSelectionNeeded() {
		return false;
	}
}

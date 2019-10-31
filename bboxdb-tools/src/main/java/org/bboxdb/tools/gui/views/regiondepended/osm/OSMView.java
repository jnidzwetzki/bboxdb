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
package org.bboxdb.tools.gui.views.regiondepended.osm;

import java.awt.BorderLayout;

import javax.swing.JButton;
import javax.swing.JPanel;

import org.bboxdb.tools.gui.GuiModel;
import org.bboxdb.tools.gui.util.MapViewerFactory;
import org.bboxdb.tools.gui.views.View;
import org.jxmapviewer.JXMapViewer;

public class OSMView implements View {

	/**
	 * The gui model
	 */
	private final GuiModel guiModel;
	
	/**
	 * The map viewer
	 */
	private JXMapViewer mapViewer;

	public OSMView(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}
	
	@Override
	public JPanel getJPanel() {
		
		mapViewer = createMapViewer();
		
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
		 
		final JButton showHagenButton = MapViewerFactory.getShowHagenButton(mapViewer);
		buttonPanel.add(showHagenButton);
		
		final JButton showBerlinButton = MapViewerFactory.getShowBerlinButton(mapViewer);
		buttonPanel.add(showBerlinButton);
		
		final JButton showSydneyButton = MapViewerFactory.getShowSydneyButton(mapViewer);
		buttonPanel.add(showSydneyButton);

		return mainPanel;
	}
	
	/**
	 * Get an instance of the map viewer
	 * @return
	 */
	private JXMapViewer createMapViewer() {	
		final JXMapViewer mapViewer = MapViewerFactory.createMapViewer();
		
		// The data distribution painter
		final OSMOverlayPainter distributionPainter = new OSMOverlayPainter(guiModel);
		mapViewer.setOverlayPainter(distributionPainter);
		
		return mapViewer;
	}

	@Override
	public boolean isGroupSelectionNeeded() {
		return true;
	}
}

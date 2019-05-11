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

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;

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


	public QueryView(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}
	
	@Override
	public JComponent getJPanel() {
		mapViewer = MapViewerFactory.createMapViewer();
		
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

		return mainPanel;	
	}

	@Override
	public boolean isGroupSelectionNeeded() {
		return false;
	}
}

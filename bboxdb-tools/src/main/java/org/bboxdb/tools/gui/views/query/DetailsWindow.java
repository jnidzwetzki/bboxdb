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

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import org.bboxdb.commons.math.GeoJsonPolygon;

public class DetailsWindow {
	
	/**
	 * The polygon to show
	 */
	private final GeoJsonPolygon polygon;

	/**
	 * The mainframe
	 */
	private final JFrame mainframe;
	
	public DetailsWindow(final GeoJsonPolygon polygon) {
		this.polygon = polygon;	
		this.mainframe = new JFrame("BBoxDB - Result details for polygon: " + polygon.getId());
	}

	/**
	 * Get the close button
	 * @return
	 */
	private JButton getCloseButton() {
		final JButton closeButton = new JButton("Close");
		closeButton.addActionListener(l -> mainframe.setVisible(false));
		return closeButton;
	}
	
	/**
	 * Show the frame
	 */
	public void show() {
		
		final JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
				
		final JComponent jTable = buildJTextArea();
		mainPanel.add(jTable, BorderLayout.CENTER);
		
		final JButton closeButton = getCloseButton();
		mainPanel.add(closeButton, BorderLayout.SOUTH);
		
		mainPanel.setPreferredSize(new Dimension(800, 600));

		mainframe.add(mainPanel);
		
		mainframe.pack();
		mainframe.setLocationRelativeTo(null);
		mainframe.setVisible(true);
	}

	/**
	 * Build the main text area
	 * @return
	 */
	private JComponent buildJTextArea() {
		final JTextArea jtextArea = new JTextArea();
		
		jtextArea.setText(polygon.toFormatedGeoJson());
		
		final JScrollPane scrollPane = new JScrollPane(jtextArea);
		
		SwingUtilities.invokeLater(() -> {
			scrollPane.getVerticalScrollBar().setValue(0);
		});
		
		return scrollPane;
	}
}

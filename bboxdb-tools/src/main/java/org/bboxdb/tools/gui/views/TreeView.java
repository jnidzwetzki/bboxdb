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
package org.bboxdb.tools.gui.views;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.tools.gui.GuiModel;

public class TreeView implements View {

	/**
	 * The gui model
	 */
	private final GuiModel guiModel;

	public TreeView(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}

	@Override
	public JComponent getJPanel() {
		final JPanel mainPanel = new JPanel();

		mainPanel.setLayout(new BorderLayout());
		final TreeJPanel jTreeComponent = new TreeJPanel(guiModel);

		final JScrollPane rightScrollPanel = new JScrollPane(jTreeComponent);
		mainPanel.setPreferredSize(new Dimension(1000, 600));
		jTreeComponent.setPreferredSize(new Dimension(800, 550));
		
		mainPanel.add(rightScrollPanel, BorderLayout.CENTER);

		final JPanel buttonPanel = new JPanel();
		mainPanel.add(buttonPanel, BorderLayout.SOUTH);
		
		final JTextField textField = new JTextField();
		textField.setText(Double.toString(MathUtil.round(jTreeComponent.getZoomFactor(), 1)));
		
		final JButton zoomInButton = getZoomInButton(jTreeComponent, textField);
		buttonPanel.add(zoomInButton);

		final JButton zoomOutButton = getZoomOutButton(jTreeComponent, textField);		
		buttonPanel.add(zoomOutButton);
		buttonPanel.add(textField);
		
		return mainPanel;
	}
	
	/**
	 * Get the zoom in button
	 * @param textField 
	 * @param jTreeComponent 
	 * @return
	 */
	protected JButton getZoomInButton(final TreeJPanel jTreeComponent, final JTextField textField) {
		final JButton zoomInButton = new JButton("Zoom in");
		zoomInButton.addActionListener((l) -> {
			jTreeComponent.setZoomFactor(jTreeComponent.getZoomFactor() + 0.1);
			textField.setText(Double.toString(jTreeComponent.getZoomFactor()));
			textField.setText(Double.toString(MathUtil.round(jTreeComponent.getZoomFactor(), 1)));
		}
		); 
		return zoomInButton;
	}

	/**
	 * Get the zoom out button
	 * @param textField 
	 * @param jTreeComponent 
	 * @return
	 */
	protected JButton getZoomOutButton(TreeJPanel jTreeComponent, JTextField textField) {
		final JButton zoomOutButton = new JButton("Zoom out");
		zoomOutButton.addActionListener((l) -> {
			jTreeComponent.setZoomFactor(jTreeComponent.getZoomFactor() - 0.1);
			textField.setText(Double.toString(MathUtil.round(jTreeComponent.getZoomFactor(), 1)));
		}
		); 
		return zoomOutButton;
	}
}

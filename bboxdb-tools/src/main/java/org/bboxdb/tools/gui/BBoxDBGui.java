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
package org.bboxdb.tools.gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.RootPaneContainer;
import javax.swing.SwingConstants;
import javax.swing.border.BevelBorder;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.tools.gui.views.View;
import org.bboxdb.tools.gui.views.ViewMode;
import org.bboxdb.tools.gui.views.query.QueryView;
import org.bboxdb.tools.gui.views.regiondepended.osm.OSMView;
import org.bboxdb.tools.gui.views.regiondepended.tree.TreeView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBGui {

	/**
	 * The main frame
	 */
	private JFrame mainframe;

	/**
	 * The main panel
	 */
	private JComponent mainPanel;

	/**
	 * The Menu bar
	 */
	private JMenuBar menuBar;
	
	/**
	 * The status label
	 */
	private JLabel statusLabel;

	/**
	 * The table Model
	 */
	private BBoxDBInstanceTableModel tableModel;

	/**
	 * The GUI Model
	 */
	private GuiModel guiModel;
	
	/**
	 * The view mode
	 */
	private ViewMode viewMode = ViewMode.TREE_MODE;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(BBoxDBGui.class);

	public BBoxDBGui(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}

	/**
	 * Build the BBoxDB dialog, init GUI components
	 * and assemble the dialog
	 */
	public void run() {

		mainframe = new JFrame("BBoxDB - GUI Client");

		setupMenu();
		mainPanel = buildSplitPane();
		updateMainPanel();


		tableModel = getTableModel();
		final JTable table = new JTable(tableModel);
		table.getColumnModel().getColumn(0).setMaxWidth(40);
		table.getColumnModel().getColumn(2).setMinWidth(100);
		table.getColumnModel().getColumn(2).setMaxWidth(100);

		table.setDefaultRenderer(Object.class, new InstanceTableRenderer());

		final JScrollPane tableScrollPane = new JScrollPane(table);		
		final Dimension dimension = table.getPreferredSize();

		if(dimension != null) {
			tableScrollPane.setPreferredSize(
					new Dimension(dimension.width, table.getRowHeight() * 7));
		}

		mainframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainframe.setLayout(new BorderLayout());
		mainframe.add(mainPanel, BorderLayout.CENTER);

		final JPanel southPanel = new JPanel();
		southPanel.setLayout(new BorderLayout());
		southPanel.add(tableScrollPane, BorderLayout.CENTER);

		final JPanel statusPanel = new JPanel();
		statusPanel.setBorder(new BevelBorder(BevelBorder.LOWERED));
		statusPanel.setPreferredSize(new Dimension(southPanel.getWidth(), 20));
		statusPanel.setLayout(new BoxLayout(statusPanel, BoxLayout.X_AXIS));
		statusLabel = new JLabel("");
		statusLabel.setHorizontalAlignment(SwingConstants.LEFT);
		statusPanel.add(statusLabel);
		
		southPanel.add(statusPanel, BorderLayout.SOUTH);
		mainframe.add(southPanel, BorderLayout.SOUTH);
		mainframe.pack();
		
		mainframe.setLocationRelativeTo(null);
		mainframe.setVisible(true);
	}

	/** 
	 * Dispose the main frame
	 */
	public void dispose() {
		mainframe.dispose();
	}

	/**
	 * Get the table model for the schedules queries
	 * @return The table model
	 */
	private BBoxDBInstanceTableModel getTableModel() {
		final List<BBoxDBInstance> bboxDBInstances = guiModel.getBBoxDBInstances();
		return new BBoxDBInstanceTableModel(bboxDBInstances, guiModel);
	}

	/**
	 * Initialize the GUI panel
	 * @return 
	 */
	private JSplitPane buildSplitPane() {
		final JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
		splitPane.setLeftComponent(getLeftPanel());
		splitPane.setOneTouchExpandable(true);
		splitPane.setDividerLocation(150);
		
		return splitPane;
	}

	/**
	 * Get the view for the right panel
	 * @return
	 */
	private View getRightPanel() {
		switch(viewMode) {
			case TREE_MODE:
				return new TreeView(guiModel);
			case OSM_MODE:
				return new OSMView(guiModel);
			case QUERY_MODE:
				return new QueryView(guiModel);
			default:
				throw new IllegalArgumentException("Unknown mode: " + viewMode);
		}
	}
	
	/**
	 * Update the main panel
	 */
	private void updateMainPanel() {

		final View view = getRightPanel();
		final JComponent viewPanel = view.getJPanel();

		if(! view.isGroupSelectionNeeded()) {
			statusLabel.setText("");
			mainframe.remove(mainPanel);
			mainPanel = viewPanel;
			mainframe.add(mainPanel, BorderLayout.CENTER);
			mainframe.revalidate();
		} else {
			JSplitPane pane = null;
			
			if(mainPanel instanceof JSplitPane) {
				pane = (JSplitPane) mainPanel;
				
				int oldLocation = pane.getDividerLocation();
				pane.setRightComponent(viewPanel);
				pane.setDividerLocation(oldLocation);
				
				mainframe.revalidate();
			} else {
				pane = buildSplitPane();
				pane.setRightComponent(viewPanel);
				mainframe.remove(mainPanel);
				mainframe.add(pane, BorderLayout.CENTER);
				mainPanel = pane;
				mainframe.revalidate();
			}			
		}
	}

	/**
	 * Generate the left panel
	 * @return
	 */
	private JComponent getLeftPanel() {
		
		final DefaultListModel<String> listModel = new DefaultListModel<String>();
		final JList<String> distributionGroupList = new JList<String>(listModel);

		refreshDistributionGroups(listModel);

		distributionGroupList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		distributionGroupList.setSelectedValue(guiModel.getDistributionGroup(), true);
		distributionGroupList.addListSelectionListener(e -> {
			if (! e.getValueIsAdjusting()) {
				guiModel.setDistributionGroup(distributionGroupList.getSelectedValue());
				updateMainPanel();
			}
		});
		
		final JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());
		panel.add(distributionGroupList, BorderLayout.CENTER);

		final JButton reloadItem = new JButton("Reload");
		reloadItem.addActionListener((e) ->  {
				refreshDistributionGroups(listModel);
				updateMainPanel();
		});
		panel.add(reloadItem, BorderLayout.SOUTH);

		return panel;
	}

	/**
	 * Refresh the distributions groups
	 * @param listModel
	 */
	private void refreshDistributionGroups(final DefaultListModel<String> listModel) {
		final List<String> distributionGroups = new ArrayList<>();

		try {
			distributionGroups.addAll(guiModel.getDistributionGroups());
		} catch (Exception e) {
			logger.error("Got an exception while loading distribution groups");
		}

		Collections.sort(distributionGroups);
		listModel.clear();
		
		for(final String distributionGroupName : distributionGroups) {
			listModel.addElement(distributionGroupName);
		}
		
		guiModel.setDistributionGroup(null);
		guiModel.unregisterTreeChangeListener();
		
		if(statusLabel != null) {
			statusLabel.setText("");
		}
	}

	/**
	 * Create the menu of the main window
	 */
	private void setupMenu() {
		menuBar = new JMenuBar();
		mainframe.setJMenuBar(menuBar);

		// File menu
		final JMenu menu = new JMenu("File");
		menuBar.add(menu);
				
		final JMenuItem screeenshotMode = new JMenuItem("Toggle screenshot mode");
		menu.add(screeenshotMode);
		screeenshotMode.addActionListener((e) -> {
			guiModel.setScreenshotMode(! guiModel.isScreenshotMode());
			tableModel.fireTableDataChanged();
		});
		

		final JMenuItem closeItem = new JMenuItem("Close");
		menu.add(closeItem);
		closeItem.addActionListener((e) -> {
				System.exit(0);
		});
		
		// View menu
		final JMenu view = new JMenu("View");
		menuBar.add(view);

		final JMenuItem viewKDTree = new JMenuItem("Tree view");
		view.add(viewKDTree);
		viewKDTree.addActionListener((e) -> {
			viewMode = ViewMode.TREE_MODE;
			updateMainPanel();
		});
		
		final JMenuItem viewKDTreeOsm = new JMenuItem("OpenStreetMap view");
		view.add(viewKDTreeOsm);
		viewKDTreeOsm.addActionListener((e) -> {
			viewMode = ViewMode.OSM_MODE;
			updateMainPanel();
		});
		
		final JMenuItem queryMode = new JMenuItem("Open query view");
		view.add(queryMode);
		queryMode.addActionListener((e) -> {
			viewMode = ViewMode.QUERY_MODE;
			updateMainPanel();
		});
	}

	/**
	 * Update the view. This method should be called periodically
	 */
	public void updateView() {
		if(tableModel != null) {
			tableModel.fireTableDataChanged();
		}

		mainframe.repaint();
	}
	
	/**
	 * Get the glass pane of the main panel
	 * @return
	 */
	public Component getGlassPane() {
		
		if(mainPanel == null) {
			return null;
		}
		
		final RootPaneContainer root = 
				(RootPaneContainer) mainPanel.getTopLevelAncestor();
		   
		if(root == null) {
			return null;
		}
		
		return root.getGlassPane();
	}
	
	/**
	 * Returns the status label
	 * @return
	 */
	public JLabel getStatusLabel() {
		return statusLabel;
	}
}
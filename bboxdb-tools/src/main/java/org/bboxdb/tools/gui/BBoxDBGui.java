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
package org.bboxdb.tools.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
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

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.tools.gui.views.KDTreeOSMView;
import org.bboxdb.tools.gui.views.KDTreeView;
import org.bboxdb.tools.gui.views.View;
import org.bboxdb.tools.gui.views.ViewMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BBoxDBGui {

	/**
	 * The main frame
	 */
	protected JFrame mainframe;

	/**
	 * The main panel
	 */
	protected JSplitPane mainPanel;

	/**
	 * The list model for the distribution groups
	 */
	protected DefaultListModel<String> listModel;

	/**
	 * The left menu list
	 */
	protected JList<String> leftList;

	/**
	 * The Menu bar
	 */
	protected JMenuBar menuBar;
	
	/**
	 * The status label
	 */
	protected JLabel statusLabel;

	/**
	 * The table Model
	 */
	protected BBoxDBInstanceTableModel tableModel;

	/**
	 * The GUI Model
	 */
	protected GuiModel guiModel;
	
	/**
	 * The view mode
	 */
	protected ViewMode viewMode = ViewMode.KD_TREE_MODE;

	/**
	 * The Logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(BBoxDBGui.class);

	public BBoxDBGui(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}

	/**
	 * Build the BBoxDB dialog, init GUI components
	 * and assemble the dialog
	 */
	public void run() {

		mainframe = new JFrame("BBoxDB - Data Distribution");

		setupMenu();
		buildMainPanel();

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

		final JPanel statusPanel = new JPanel();
		statusPanel.setBorder(new BevelBorder(BevelBorder.LOWERED));
		statusPanel.setPreferredSize(new Dimension(southPanel.getWidth(), 20));
		statusPanel.setLayout(new BoxLayout(statusPanel, BoxLayout.X_AXIS));
		statusLabel = new JLabel("");
		statusLabel.setHorizontalAlignment(SwingConstants.LEFT);
		statusPanel.add(statusLabel);
		
		southPanel.setLayout(new BorderLayout());
		southPanel.add(tableScrollPane, BorderLayout.CENTER);
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
		return new BBoxDBInstanceTableModel(bboxDBInstances);
	}

	/**
	 * Initialize the GUI panel
	 * 
	 */
	protected void buildMainPanel() {
		
		final JScrollPane rightScrollPanel = getRightPanel();

		final JList<String> leftPanel = getLeftPanel();

		mainPanel = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPanel, rightScrollPanel);
		mainPanel.setOneTouchExpandable(true);
		mainPanel.setDividerLocation(150);
		mainPanel.setPreferredSize(new Dimension(1100, 600));		
	}

	protected JScrollPane getRightPanel() {
		View view = null;
			
		if(viewMode == ViewMode.KD_TREE_MODE) {
			view = new KDTreeView(guiModel);
		} else {
			view = new KDTreeOSMView(guiModel);
		}

		final JPanel rightPanel = view.getJPanel();
		rightPanel.setBackground(Color.WHITE);
		rightPanel.setToolTipText("");

		final JScrollPane rightScrollPanel = new JScrollPane(rightPanel);
		return rightScrollPanel;
	}
	
	/**
	 * Update the main panel
	 */
	protected void updateMainPanel() {
		final int oldLocation = mainPanel.getDividerLocation();
		mainPanel.setRightComponent(getRightPanel());
		mainPanel.setDividerLocation(oldLocation);
	}

	/**
	 * Generate the left panel
	 * @return
	 */
	protected JList<String> getLeftPanel() {

		listModel = new DefaultListModel<String>();

		leftList = new JList<String>(listModel);

		refreshDistributionGroups(listModel);

		leftList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		leftList.addListSelectionListener(e -> {
			if (! e.getValueIsAdjusting()) {
				guiModel.setDistributionGroup(leftList.getSelectedValue());
				viewMode = ViewMode.KD_TREE_MODE;
				updateMainPanel();
			}
		});

		return leftList;
	}

	/**
	 * Refresh the distributions groups
	 * @param listModel
	 */
	protected void refreshDistributionGroups(final DefaultListModel<String> listModel) {
		final List<DistributionGroupName> distributionGroups = new ArrayList<DistributionGroupName>();

		try {
			distributionGroups.addAll(guiModel.getDistributionGroups());
		} catch (Exception e) {
			logger.error("Got an exception while loading distribution groups");
		}

		Collections.sort(distributionGroups);
		listModel.clear();
		
		for(final DistributionGroupName distributionGroupName : distributionGroups) {
			listModel.addElement(distributionGroupName.getFullname());
		}
		
		if(statusLabel != null) {
			statusLabel.setText("");
		}
	}

	/**
	 * Create the menu of the main window
	 */
	protected void setupMenu() {
		menuBar = new JMenuBar();
		mainframe.setJMenuBar(menuBar);

		// File menu
		final JMenu menu = new JMenu("File");
		menuBar.add(menu);
				
		final JMenuItem reloadItem = new JMenuItem("Reload Distribution Groups");
		menu.add(reloadItem);
		reloadItem.addActionListener((e) ->  {
				refreshDistributionGroups(listModel);
		});
		

		final JMenuItem closeItem = new JMenuItem("Close");
		menu.add(closeItem);
		closeItem.addActionListener((e) -> {
				System.exit(0);
		});
		
		// View menu
		final JMenu view = new JMenu("View");
		menuBar.add(view);

		final JMenuItem viewKDTree = new JMenuItem("KD Tree view");
		view.add(viewKDTree);
		viewKDTree.addActionListener((e) -> {
			viewMode = ViewMode.KD_TREE_MODE;
			updateMainPanel();
		});
		
		final JMenuItem viewKDTreeOsm = new JMenuItem("KD Tree OSM view");
		view.add(viewKDTreeOsm);
		viewKDTreeOsm.addActionListener((e) -> {
			viewMode = ViewMode.KD_TREE_OSM_MODE;
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
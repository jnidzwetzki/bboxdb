/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class ChooseDistributionGroupDialog {

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution groups
	 */
	protected final List<DistributionGroupName> distributionGroups;
	
	/**
	 * The main frame
	 */
	protected JFrame mainframe;
	
	/**
	 * The combo box
	 */
	protected JComboBox<String> distributionGroupBox;
	
	/**
	 * The logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(ChooseDistributionGroupDialog.class);

	public ChooseDistributionGroupDialog(final List<DistributionGroupName> distributionGroups, final ZookeeperClient zookeeperClient) {
		this.distributionGroups = new ArrayList<DistributionGroupName>(distributionGroups.size());
		this.distributionGroups.addAll(distributionGroups);
		Collections.sort(this.distributionGroups);
		this.zookeeperClient = zookeeperClient;
	}

	/**
	 * Show the connect dialog
	 */
	public void showDialog() {
		mainframe = new JFrame("Scalephant - Connection Settings");
		distributionGroupBox = new JComboBox<String>();
		
		for(final DistributionGroupName distributionGroupName : distributionGroups ) {
			distributionGroupBox.addItem(distributionGroupName.getFullname());
		}
		
		final PanelBuilder builder = buildDialog();
		
		mainframe.add(builder.getPanel());
		mainframe.pack();
		GuiHelper.setCenterPosition(mainframe);
		mainframe.setVisible(true);
	}
	
	/**
	 * Build the main panel
	 * @return
	 */
	protected PanelBuilder buildDialog() {
		// Close
		final Action closeAction = getCloseAction();
		final JButton closeButton = new JButton(closeAction);
		closeButton.setText("Close");
		
		// Connect
		final Action connectAction = getConnectAction();
		final JButton connectButton = new JButton(connectAction);
		connectButton.setText("Connect");

		final FormLayout layout = new FormLayout(
			    "right:pref, 3dlu, pref", 			// columns
			    "p, 3dlu, p, 9dlu, p");	// rows
		
		final PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		
		final CellConstraints cc = new CellConstraints();
		builder.addSeparator("Distribution Groups", cc.xyw(1,  1, 3));
		builder.addLabel("Group to observe", cc.xy (1,  3));
		builder.add(distributionGroupBox, cc.xy(3, 3));
		
		builder.add(connectButton, cc.xy(1, 5));
		builder.add(closeButton, cc.xy(3, 5));
		return builder;
	}

	/**
	 * Returns the close action
	 * @return
	 */
	protected AbstractAction getCloseAction() {
		return new AbstractAction() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -1349800056154800682L;

			@Override
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
			
		};
	}
	
	/**
	 * Get the connect action
	 * @return
	 */
	protected Action getConnectAction() {
		final AbstractAction connectAction = new AbstractAction() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 2908534701228350424L;

			@Override
			public void actionPerformed(ActionEvent e) {
				final String distributionGroup = (String) distributionGroupBox.getSelectedItem();
				showMainDialog(distributionGroup, zookeeperClient);
				
				zookeeperClient.startMembershipObserver();
				mainframe.dispose();
			}

			/**
			 * Show the main dialog
			 * 
			 * @param distributionGroup
			 * @param zookeeperClient
			 */
			protected void showMainDialog(final String distributionGroup,
					final ZookeeperClient zookeeperClient) {
				
				final GuiModel guiModel = new GuiModel(zookeeperClient);		
				final BBoxDBGui scalepahntGUI = new BBoxDBGui(guiModel);
				guiModel.setBBoxDBGui(scalepahntGUI);
				scalepahntGUI.run();
				guiModel.setDistributionGroup(distributionGroup);
				
				startNewMainThread(zookeeperClient, scalepahntGUI, guiModel);
			}

			/**
			 * The main thread
			 * @param zookeeperClient
			 * @param scalepahntGUI
			 * @param guiModel 
			 */
			protected void startNewMainThread(
					final ZookeeperClient zookeeperClient,
					final BBoxDBGui scalepahntGUI,
					final GuiModel guiModel) {
				
				// Start a new update thread
				(new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							while(! scalepahntGUI.shutdown) {
								try {
									guiModel.updateDistributionRegion();
								} catch (ZookeeperException | ZookeeperNotFoundException e) {
									logger.warn("Got exception: ", e);
								}
								scalepahntGUI.updateView();
								Thread.sleep(1000);
							}
							
							// Wait for pending gui updates to complete
							scalepahntGUI.dispose();				
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							Thread.currentThread().interrupt();
						}
						
						zookeeperClient.shutdown();
					}
				})).start();
			}
		};
		return connectAction;
	}
}

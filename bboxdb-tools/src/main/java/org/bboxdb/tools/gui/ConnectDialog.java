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

import java.awt.event.ActionEvent;
import java.util.Arrays;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JTextField;

import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class ConnectDialog {

	/**
	 * The default name of the cluster
	 */
	protected static final String DEFAULT_CLUSTER = "mycluster";
	
	/**
	 * The default name of the nodes
	 */
	protected static final String DEFAULT_NODE = "node1";

	/**
	 * The hosts field
	 */
	protected JTextField hosts;

	/**
	 * The clustername field
	 */
	protected JTextField clustername;

	/**
	 * The main frame
	 */
	protected JFrame mainframe;
	
	/**
	 * The logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(ConnectDialog.class);
	

	/**
	 * Show the connect dialog
	 */
	public void showDialog() {
		mainframe = new JFrame("BBoxDB - Connection Settings");
		hosts = new JTextField(DEFAULT_NODE);
		clustername = new JTextField(DEFAULT_CLUSTER);
		
		final PanelBuilder builder = buildDialog();
		
		mainframe.add(builder.getPanel());
		mainframe.pack();
		mainframe.setLocationRelativeTo(null);
		mainframe.setVisible(true);
	}

	/**
	 * Build the dialog
	 * @return
	 */
	protected PanelBuilder buildDialog() {
		
		// Close
		final JButton closeButton = new JButton();
		closeButton.setText("Close");
		closeButton.addActionListener((e) -> {
			System.exit(0);
		});
		
		// Connect
		final Action connectAction = getConnectAction();
		final JButton connectButton = new JButton(connectAction);
		connectButton.setText("Connect");

		final FormLayout layout = new FormLayout(
			    "right:pref, 3dlu, 60dlu", 			// columns
			    "p, 3dlu, p, 3dlu, p, 9dlu, p");	// rows
		
		final PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		
		final CellConstraints cc = new CellConstraints();
		builder.addSeparator("Connection", cc.xyw(1,  1, 3));
		builder.addLabel("Zookeeper Hosts", cc.xy (1,  3));
		builder.add(hosts, cc.xy(3, 3));
		builder.addLabel("Clustername", cc.xy (1,  5));
		builder.add(clustername, cc.xy(3, 5));
		
		builder.add(connectButton, cc.xy(1, 7));
		builder.add(closeButton, cc.xy(3, 7));
		return builder;
	}

	/**
	 * Get the connect action
	 * @return
	 */
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
				final String guiHosts = hosts.getText();
				
				if(guiHosts == null) {
					return;
				}
				
				final String[] hostList = guiHosts.split(",");
				final List<String> zookeeperHosts = Arrays.asList(hostList);
				final String cluster = clustername.getText();
				
				final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeeperHosts, cluster);
				zookeeperClient.init();
				
				showMainDialog(zookeeperClient);
				
				BBoxDBInstanceManager.getInstance().startMembershipObserver(zookeeperClient);
				
				mainframe.dispose();
			}

			/**
			 * Show the main dialog
			 * 
			 * @param distributionGroup
			 * @param zookeeperClient
			 */
			protected void showMainDialog(final ZookeeperClient zookeeperClient) {
				final GuiModel guiModel = new GuiModel(zookeeperClient);		
				final BBoxDBGui bboxDBGUI = new BBoxDBGui(guiModel);
				guiModel.setBBoxDBGui(bboxDBGUI);
				bboxDBGUI.run();				
			}
			
		};
		return connectAction;
	}
}

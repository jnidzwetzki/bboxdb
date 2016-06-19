package de.fernunihagen.dna.jkn.scalephant.tools.gui;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionGroupName;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;

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
	private JTextField hosts;

	/**
	 * The clustername field
	 */
	private JTextField clustername;

	/**
	 * The main frame
	 */
	private JFrame mainframe;
	
	/**
	 * The logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(ConnectDialog.class);
	

	/**
	 * Show the connect dialog
	 */
	public void showDialog() {
		mainframe = new JFrame("Scalephant - Connection Settings");
		hosts = new JTextField(DEFAULT_NODE);
		clustername = new JTextField(DEFAULT_CLUSTER);
		
		final PanelBuilder builder = buildDialog();
		
		mainframe.add(builder.getPanel());
		mainframe.pack();
		GuiHelper.setCenterPosition(mainframe);
		mainframe.setVisible(true);
	}

	/**
	 * Build the dialog
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
			    "p, 3dlu, p, 3dlu, p, 9dlu, p");	// rows
		
		final PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		
		final CellConstraints cc = new CellConstraints();
		builder.addSeparator("Connection", cc.xyw(1,  1, 3));
		builder.addLabel("Zookeeper Hosts", cc.xy (1,  3));
		builder.add(hosts, cc.xy(3, 3));
		builder.addLabel("Clustername", cc.xy (1,  5));
		builder.add(clustername, cc.xy(3, 5));
		
		builder.add(closeButton, cc.xy(1, 7));
		builder.add(connectButton, cc.xy(3, 7));
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
				
				final List<String> zookeeperHosts = Arrays.asList(hosts.getText().split(","));
				final String cluster = clustername.getText();
				
				final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeeperHosts, cluster);
				zookeeperClient.init();
				
				if(! zookeeperClient.isConnected()) {
					JOptionPane.showMessageDialog(mainframe,
						    "Unable to connect to zookeeper.",
						    "Error",
						    JOptionPane.ERROR_MESSAGE);
					return;
				}
				
				try {
					final String distributionGroup = getDistributioGroup(zookeeperClient); 
					zookeeperClient.startMembershipObserver();
					mainframe.dispose();
					showMainDialog(distributionGroup, zookeeperClient);
				} catch (ZookeeperException e1) {
					logger.error("Got an exception", e1);
				}
			}

			/**
			 * Get the distribution group to display
			 * @param zookeeperClient
			 * @return
			 * @throws ZookeeperException
			 */
			protected String getDistributioGroup(
					final ZookeeperClient zookeeperClient)
					throws ZookeeperException {
				
				final List<DistributionGroupName> distributionGroups = zookeeperClient.getDistributionGroups();
				
				if(distributionGroups.isEmpty()) {
					JOptionPane.showMessageDialog(mainframe,
						    "No distribution groups are found. Please create them first.",
						    "Distribution Groups",
						    JOptionPane.ERROR_MESSAGE);
					System.exit(0);
				}
				
				final List<String> groupNames = new ArrayList<String>();
				
				for(final DistributionGroupName name: distributionGroups) {
					groupNames.add(name.getFullname());
				}
				
				final String distributionGroup = (String) JOptionPane.showInputDialog(mainframe, "Distribution Group",
				        "Choose the distribution group", JOptionPane.QUESTION_MESSAGE, null,
				        groupNames.toArray(), 
				        0);
				
				return distributionGroup;
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
				final ScalephantGui scalepahntGUI = new ScalephantGui(guiModel);
				guiModel.setScalephantGui(scalepahntGUI);
				scalepahntGUI.run();
				guiModel.setDistributionGroup(distributionGroup);
				
				startNewMainThread(zookeeperClient, scalepahntGUI);
			}

			/**
			 * The main thread
			 * @param zookeeperClient
			 * @param scalepahntGUI
			 */
			protected void startNewMainThread(
					final ZookeeperClient zookeeperClient,
					final ScalephantGui scalepahntGUI) {
				
				// Start a new update thread
				(new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							while(! scalepahntGUI.shutdown) {
								scalepahntGUI.updateView();
								Thread.sleep(1000);
							}
							
							// Wait for pending gui updates to complete
							scalepahntGUI.dispose();				
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							// Ignore exception
						}
						
						zookeeperClient.shutdown();
					}
				})).start();
			}
		};
		return connectAction;
	}
}

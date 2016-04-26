package de.fernunihagen.dna.jkn.scalephant.tools.gui;

import java.util.Arrays;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;

public class Main {
	
	/**
	 * Main Method 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		final List<String> zookeeperHosts = Arrays.asList(new String[] {"node1"});
		final String clustername = "";
		
		final ZookeeperClient zookeeperClient = new ZookeeperClient(zookeeperHosts, clustername);
		zookeeperClient.init();
		
		final GUIModel guiModel = new GUIModel(zookeeperClient);
		guiModel.updateModel();
		
		final ScalephantGUI cassandraGUI = new ScalephantGUI(guiModel);
		cassandraGUI.run();
		
		while(! cassandraGUI.shutdown) {
			cassandraGUI.updateView();
			Thread.sleep(1000);
		}
		
		cassandraGUI.dispose();
		
		// Wait for pending gui updates to complete
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// Ignore exception
		}
		
		zookeeperClient.shutdown();
	}
}

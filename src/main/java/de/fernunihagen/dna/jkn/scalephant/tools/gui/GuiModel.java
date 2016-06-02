package de.fernunihagen.dna.jkn.scalephant.tools.gui;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionRegion;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperException;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEventCallback;

public class GuiModel implements DistributedInstanceEventCallback {
	
	/**
	 * The scalephant instances
	 */
	protected final List<DistributedInstance> scalephantInstances;
	
	/**
	 * The distribution group to display
	 */
	protected String distributionGroup;
	
	/**
	 * The distribution region
	 */
	protected DistributionRegion rootRegion;
	
	/**
	 * The reference to the gui window
	 */
	protected ScalephantGui scalephantGui;
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient client;

	
	protected final static Logger logger = LoggerFactory.getLogger(GuiModel.class);

	public GuiModel(final ZookeeperClient client) {
		super();
		this.client = client;
		scalephantInstances = new ArrayList<DistributedInstance>();
		
		DistributedInstanceManager.getInstance().registerListener(this);
	}

	/**
	 * Shutdown the GUI model
	 */
	public void shutdown() {
		DistributedInstanceManager.getInstance().removeListener(this);
	}
	
	/**
	 * Update the GUI model
	 */
	public synchronized void updateModel() {
		try {
			updateScalepahntInstances();
			updateDistributionRegion();
			scalephantGui.updateView();
		} catch(Exception e) {
			logger.info("Exception while updating the view", e);
		}
	}
	

	/**
	 * Update the system state
	 */
	protected void updateScalepahntInstances() {
		scalephantInstances.clear();
		scalephantInstances.addAll(DistributedInstanceManager.getInstance().getInstances());
	}
	
	/**
	 * Update the distribution region
	 * @throws ZookeeperException 
	 */
	protected void updateDistributionRegion() throws ZookeeperException {
		rootRegion = client.readDistributionGroup(distributionGroup);
	}

	/**
	 * A group membership event is occurred
	 */
	@Override
	public void distributedInstanceEvent(final DistributedInstanceEvent event) {
		updateScalepahntInstances();
	}

	/**
	 * Get the scalephant instances
	 * @return
	 */
	public List<DistributedInstance> getScalephantInstances() {
		return scalephantInstances;
	}

	/**
	 * Set the gui component
	 * @param scalephantGui
	 */
	public void setScalephantGui(final ScalephantGui scalephantGui) {
		this.scalephantGui = scalephantGui;
	}

	/**
	 * Get the distribution group
	 * 
	 * @return
	 */
	public String getDistributionGroup() {
		return distributionGroup;
	}

	/**
	 * Set the distribution group
	 * 
	 * @param distributionGroup
	 */
	public void setDistributionGroup(final String distributionGroup) {
		this.distributionGroup = distributionGroup;
		
		// Display the new distribution group
		updateModel();
	}

}

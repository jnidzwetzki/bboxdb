package de.fernunihagen.dna.jkn.scalephant.tools.gui;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.ZookeeperClient;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstanceManager;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEventCallback;

public class GUIModel implements DistributedInstanceEventCallback {
	
	/**
	 * The scalephant instances
	 */
	protected final List<DistributedInstance> scalephantInstances;
	
	/**
	 * The reference to the gui window
	 */
	protected ScalephantGUI scalephantGui;
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient client;

	
	protected final static Logger logger = LoggerFactory.getLogger(GUIModel.class);

	public GUIModel(final ZookeeperClient client) {
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
		} catch(Exception e) {
			logger.info("Exception while updating view", e);
		}
	}
	

	/**
	 * Update the system state
	 */
	private void updateScalepahntInstances() {
		scalephantInstances.clear();
		scalephantInstances.addAll(DistributedInstanceManager.getInstance().getInstances());
		scalephantGui.updateView();
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
	public void setScalephantGui(final ScalephantGUI scalephantGui) {
		this.scalephantGui = scalephantGui;
	}

}

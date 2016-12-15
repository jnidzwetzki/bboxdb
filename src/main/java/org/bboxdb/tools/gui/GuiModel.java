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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.DistributedInstanceManager;
import org.bboxdb.distribution.membership.event.DistributedInstanceEvent;
import org.bboxdb.distribution.membership.event.DistributedInstanceEventCallback;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuiModel implements DistributedInstanceEventCallback {
	
	/**
	 * The BBoxDB instances
	 */
	protected final List<DistributedInstance> bboxdbInstances;
	
	/**
	 * The distribution group to display
	 */
	protected String distributionGroup;
	
	/**
	 * The distribution region
	 */
	protected DistributionRegion rootRegion;
	
	/**
	 * The replication factor for the distribution group
	 */
	protected short replicationFactor;
	
	/**
	 * The version of the root region
	 */
	protected String rootRegionVersion;
	
	/**
	 * The reference to the gui window
	 */
	protected BBoxDBGui bboxdbGui;
	
	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;
	
	/**
	 * The distribution group adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;

	
	protected final static Logger logger = LoggerFactory.getLogger(GuiModel.class);

	public GuiModel(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(zookeeperClient);
		bboxdbInstances = new ArrayList<DistributedInstance>();
		
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
	public void updateModel() {
		try {
			updateBBoxDBInstances();
			bboxdbGui.updateView();
		} catch(Exception e) {
			logger.info("Exception while updating the view", e);
		}
	}
	

	/**
	 * Update the system state
	 */
	protected void updateBBoxDBInstances() {
		synchronized (bboxdbInstances) {		
			bboxdbInstances.clear();
			bboxdbInstances.addAll(DistributedInstanceManager.getInstance().getInstances());
			Collections.sort(bboxdbInstances);
		}
	}
	
	/**
	 * Update the distribution region
	 * @throws ZookeeperException 
	 * @throws ZookeeperNotFoundException 
	 */
	public void updateDistributionRegion() throws ZookeeperException, ZookeeperNotFoundException {
		final String currentVersion = distributionGroupZookeeperAdapter.getVersionForDistributionGroup(distributionGroup, null);
		
		if(! currentVersion.equals(rootRegionVersion)) {
			logger.info("Reread distribution group, version has changed: " + rootRegionVersion + " / " + currentVersion);
			final KDtreeZookeeperAdapter adapter = distributionGroupZookeeperAdapter.readDistributionGroup(distributionGroup);
			rootRegion = adapter.getRootNode();
			rootRegionVersion = currentVersion;
			replicationFactor = distributionGroupZookeeperAdapter.getReplicationFactorForDistributionGroup(distributionGroup);
		}
	}

	/**
	 * A group membership event is occurred
	 */
	@Override
	public void distributedInstanceEvent(final DistributedInstanceEvent event) {
		updateBBoxDBInstances();
	}

	/**
	 * Get the bboxdb instances
	 * @return
	 */
	public List<DistributedInstance> getBBoxDBInstances() {
		return bboxdbInstances;
	}

	/**
	 * Set the gui component
	 * @param bboxDBGui
	 */
	public void setBBoxDBGui(final BBoxDBGui bboxDBGui) {
		this.bboxdbGui = bboxDBGui;
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
	 * Get the name of the cluster
	 * @return
	 */
	public String getClustername() {
		return zookeeperClient.getClustername();
	}
	
	/**
	 * Get the replication factor
	 * @return
	 */
	public short getReplicationFactor() {
		return replicationFactor;
	}

	/**
	 * Set the replication factor
	 * @param replicationFactor
	 */
	public void setReplicationFactor(final short replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	/**
	 * Set the distribution group
	 * 
	 * @param distributionGroup
	 */
	public void setDistributionGroup(final String distributionGroup) {
		this.distributionGroup = distributionGroup;
	
		try {
			updateDistributionRegion();
		} catch(Exception e) {
			logger.info("Exception while updating the view", e);
		}
		
		// Display the new distribution group
		updateModel();
	}
	
	/**
	 * Get the root region
	 * @return
	 */
	public DistributionRegion getRootRegion() {
		return rootRegion;
	}

}

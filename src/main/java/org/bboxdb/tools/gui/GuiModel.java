/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.awt.Cursor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.SwingUtilities;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.distribution.membership.DistributedInstanceManager;
import org.bboxdb.distribution.membership.event.DistributedInstanceEvent;
import org.bboxdb.distribution.membership.event.DistributedInstanceEventCallback;
import org.bboxdb.distribution.mode.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.mode.DistributionRegionChangedCallback;
import org.bboxdb.distribution.mode.KDtreeZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuiModel implements DistributedInstanceEventCallback,
		DistributionRegionChangedCallback {

	/**
	 * The BBoxDB instances
	 */
	protected final List<DistributedInstance> bboxdbInstances;

	/**
	 * The distribution group to display
	 */
	protected String distributionGroup;

	/**
	 * The replication factor for the distribution group
	 */
	protected short replicationFactor;

	/**
	 * The reference to the gui window
	 */
	protected BBoxDBGui bboxdbGui;

	/**
	 * The zookeeper client
	 */
	protected final ZookeeperClient zookeeperClient;

	/**
	 * The tree adapter
	 */
	private KDtreeZookeeperAdapter treeAdapter;

	/**
	 * The distribution group adapter
	 */
	protected final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter;

	/**
	 * The logger
	 */
	protected final static Logger logger = LoggerFactory
			.getLogger(GuiModel.class);

	public GuiModel(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupZookeeperAdapter = new DistributionGroupZookeeperAdapter(
				zookeeperClient);
		bboxdbInstances = new ArrayList<DistributedInstance>();

		DistributedInstanceManager.getInstance().registerListener(this);
	}

	/**
	 * Shutdown the GUI model
	 */
	public void shutdown() {
		DistributedInstanceManager.getInstance().removeListener(this);
		unregisterTreeChangeListener();
	}

	/**
	 * Unregister the tree change listener
	 */
	protected void unregisterTreeChangeListener() {
		if (treeAdapter != null) {
			treeAdapter.unregisterCallback(this);
		}
	}

	/**
	 * Update the GUI model
	 */
	public void updateModel() {
		try {
			updateBBoxDBInstances();
			bboxdbGui.updateView();
		} catch (Exception e) {
			logger.info("Exception while updating the view", e);
		}
	}

	/**
	 * Update the system state
	 */
	protected void updateBBoxDBInstances() {
		synchronized (bboxdbInstances) {
			bboxdbInstances.clear();
			bboxdbInstances.addAll(DistributedInstanceManager.getInstance()
					.getInstances());
			Collections.sort(bboxdbInstances);
		}
	}

	/**
	 * Update the distribution region
	 * 
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public void updateDistributionRegion() throws ZookeeperException,
			ZookeeperNotFoundException {
		
		logger.info("Reread distribution group: {}", distributionGroup);
		
		// Show wait cursor
		SwingUtilities.invokeLater(() -> {
			bboxdbGui.getGlassPane().setVisible(true);
			bboxdbGui.getGlassPane().setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
		});

		// Read distribution group async
		(new Thread(() -> {
			unregisterTreeChangeListener();

			if (distributionGroup == null) {
				treeAdapter = null;
				return;
			}

			try {
				treeAdapter = distributionGroupZookeeperAdapter
						.readDistributionGroup(distributionGroup);
				
				replicationFactor = distributionGroupZookeeperAdapter
						.getReplicationFactorForDistributionGroup(distributionGroup);
			} catch (Exception e) {
				logger.warn("Got exception", e);
			}

			treeAdapter.registerCallback(GuiModel.this);
			
			logger.info("Read distribution group {} done", distributionGroup);
			
			// Reset cursor
			SwingUtilities.invokeLater(() -> {
					updateModel();
					bboxdbGui.getGlassPane().setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
					bboxdbGui.getGlassPane().setVisible(false);
			});
		})).start();
	}

	/**
	 * A group membership event is occurred
	 */
	@Override
	public void distributedInstanceEvent(final DistributedInstanceEvent event) {
		updateBBoxDBInstances();

		updateModel();
	}

	/**
	 * One of the regions was changed
	 */
	@Override
	public void regionChanged(DistributionRegion distributionRegion) {
		updateModel();
	}

	/**
	 * Get the bboxdb instances
	 * 
	 * @return
	 */
	public List<DistributedInstance> getBBoxDBInstances() {
		return bboxdbInstances;
	}

	/**
	 * Set the gui component
	 * 
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
	 * 
	 * @return
	 */
	public String getClustername() {
		return zookeeperClient.getClustername();
	}

	/**
	 * Get the replication factor
	 * 
	 * @return
	 */
	public short getReplicationFactor() {
		return replicationFactor;
	}

	/**
	 * Set the replication factor
	 * 
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
		} catch (Exception e) {
			logger.info("Exception while updating the view", e);
		}
	}

	/**
	 * Returns the tree adapter
	 * 
	 * @return
	 */
	public KDtreeZookeeperAdapter getTreeAdapter() {
		return treeAdapter;
	}

	/**
	 * Get a list with all distribution groups
	 * 
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public List<DistributionGroupName> getDistributionGroups()
			throws ZookeeperException, ZookeeperNotFoundException {
		return distributionGroupZookeeperAdapter.getDistributionGroups();
	}

}

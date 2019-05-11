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

import java.awt.Cursor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import javax.swing.SwingUtilities;

import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.membership.DistributedInstanceEvent;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.region.DistributionRegionCallback;
import org.bboxdb.distribution.region.DistributionRegionEvent;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuiModel implements DistributionRegionCallback {

	/**
	 * In screenshot mode, all IPs are replaced with 'XXXX'
	 */
	public boolean screenshotMode = false;

	/**
	 * The BBoxDB instances
	 */
	private final List<BBoxDBInstance> bboxdbInstances;

	/**
	 * The distribution group to display
	 */
	private String distributionGroup;

	/**
	 * The reference to the gui window
	 */
	private BBoxDBGui bboxdbGui;

	/**
	 * The zookeeper client
	 */
	private final ZookeeperClient zookeeperClient;

	/**
	 * The space partitioner
	 */
	private SpacePartitioner spacePartitioner;

	/**
	 * The distribution group adapter
	 */
	private final DistributionGroupAdapter distributionGroupZookeeperAdapter;
	
	/**
	 * The event handler
	 */
	private BiConsumer<DistributedInstanceEvent, BBoxDBInstance> distributedEventConsumer = (event, instance) -> {
		handleDistributedEvent(event, instance);
	};

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(GuiModel.class);

	public GuiModel(final ZookeeperClient zookeeperClient) {
		this.zookeeperClient = zookeeperClient;
		this.distributionGroupZookeeperAdapter = new DistributionGroupAdapter(
				zookeeperClient);
		bboxdbInstances = new ArrayList<BBoxDBInstance>();

		BBoxDBInstanceManager.getInstance().registerListener(distributedEventConsumer);
	}

	/**
	 * Shutdown the GUI model
	 */
	public void shutdown() {
		BBoxDBInstanceManager.getInstance().removeListener(distributedEventConsumer);
		unregisterTreeChangeListener();
	}

	/**
	 * Unregister the tree change listener
	 */
	public void unregisterTreeChangeListener() {
		if (spacePartitioner != null) {
			spacePartitioner.unregisterCallback(this);
			spacePartitioner = null;
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
	private void updateBBoxDBInstances() {
		synchronized (bboxdbInstances) {
			bboxdbInstances.clear();
			bboxdbInstances.addAll(BBoxDBInstanceManager.getInstance()
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
		
		if(distributionGroup == null) {
			return;
		}
		
		// Show wait cursor
		SwingUtilities.invokeLater(() -> {
			if(bboxdbGui.getGlassPane() != null) {
				bboxdbGui.getGlassPane().setVisible(true);
				bboxdbGui.getGlassPane().setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			}
		});

		// Read distribution group async
		(new Thread(() -> {
			unregisterTreeChangeListener();

			if (distributionGroup == null) {
				spacePartitioner = null;
				return;
			}

			try {
				spacePartitioner = SpacePartitionerCache.getInstance()
						.getSpacePartitionerForGroupName(distributionGroup);
				
				final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
						.getInstance().getDistributionGroupConfiguration(distributionGroup);
				
				spacePartitioner.registerCallback(GuiModel.this);
				
				final StringBuilder sb = new StringBuilder();
				sb.append("Cluster name: " + getClustername());
				sb.append(", Replication factor: " + config.getReplicationFactor());
				sb.append(", Dimensions: " + config.getDimensions());
				sb.append(", Space partitioner: " + config.getSpacePartitioner());
				
				bboxdbGui.getStatusLabel().setText(sb.toString());
	
				logger.info("Read distribution group {} done", distributionGroup);
				
				// Reset cursor
				SwingUtilities.invokeLater(() -> {
						updateModel();
						if(bboxdbGui.getGlassPane() != null) {
							final Cursor defaultCursor = Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR);
							bboxdbGui.getGlassPane().setCursor(defaultCursor);
							bboxdbGui.getGlassPane().setVisible(false);
						}
				});
			} catch (Exception e) {
				logger.warn("Got exception", e);
			}
		})).start();
	}

	/**
	 * A group membership event is occurred
	 * @param instance 
	 */
	public void handleDistributedEvent(final DistributedInstanceEvent event, final BBoxDBInstance instance) {
		updateBBoxDBInstances();
		updateModel();
	}

	/**
	 * One of the regions was changed
	 */
	@Override
	public void regionChanged(final DistributionRegionEvent event, 
			final DistributionRegion distributionRegion) {
		
		updateModel();
	}

	/**
	 * Get the bboxdb instances
	 * 
	 * @return
	 */
	public List<BBoxDBInstance> getBBoxDBInstances() {
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
	public SpacePartitioner getTreeAdapter() {
		return spacePartitioner;
	}

	/**
	 * Get a list with all distribution groups
	 * 
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	public List<String> getDistributionGroups()
			throws ZookeeperException, ZookeeperNotFoundException {
		return distributionGroupZookeeperAdapter.getDistributionGroups();
	}

	/**
	 * Get the screenshot mode
	 * @return
	 */
	public boolean isScreenshotMode() {
		return screenshotMode;
	}

	/**
	 * Set the screenshot mode
	 * @param screenshotMode
	 */
	public void setScreenshotMode(final boolean screenshotMode) {
		this.screenshotMode = screenshotMode;
	}
}

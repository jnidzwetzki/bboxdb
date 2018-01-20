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

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.event.MouseEvent;
import java.awt.geom.Rectangle2D;
import java.util.Map;
import java.util.stream.Collectors;

import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DoubleInterval;
import org.bboxdb.tools.gui.views.KDTreeJPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionComponent {
	
	/**
	 * The x offset of a child (- if left child / + if right child)
	 */
	public final static int LEFT_RIGHT_OFFSET = 15;
	
	/**
	 * The distance between two levels
	 */
	public final static int LEVEL_DISTANCE = 100;

	/**
	 * The height of the box
	 */
	public final static int HEIGHT = 50;
	
	/**
	 * The width of the box
	 */
	public final static int WIDTH = 90;
	
	/**
	 * The distribution region to paint
	 */
	protected final DistributionRegion distributionRegion;

	/**
	 * The x offset
	 */
	protected int xOffset;

	/**
	 * The y offset
	 */
	protected int yOffset;
	
	/**
	 * The panel
	 */
	protected final KDTreeJPanel panel;
	
	/**
	 * The logger
	 */
	protected final static Logger logger = LoggerFactory.getLogger(ConnectDialog.class);
	

	public DistributionRegionComponent(final DistributionRegion distributionRegion, 
			final KDTreeJPanel distributionGroupJPanel) {
		
		this.distributionRegion = distributionRegion;
		this.panel = distributionGroupJPanel;

		this.xOffset = calculateXOffset();
		this.yOffset = calculateYOffset();
	}
	
	/**
	 * Calculate the x offset of the component
	 * @return
	 */
	protected int calculateXOffset() {
		int offset = panel.getRootPosX();
		
		DistributionRegion level = distributionRegion;
		
		while(level.getParent() != null) {
			
			if(level.isLeftChild()) {
				offset = offset - calculateLevelXOffset(level.getLevel());
			} else {
				offset = offset + calculateLevelXOffset(level.getLevel());
			}
			
			level = level.getParent();
		}
		
		return offset;
	}
	
	/**
	 * Calculate the x offset for a given level
	 * @param level
	 * @return
	 */
	protected int calculateLevelXOffset(final int level) {
		
		final int offsetLastLevel = (LEFT_RIGHT_OFFSET + WIDTH) / 2;
		
		final int curentLevel = distributionRegion.getTotalLevel() - level - 1;
		
		return (int) (offsetLastLevel * Math.pow(2, curentLevel));
	}
	
	/**
	 * Calculate the Y offset
	 * @return
	 */
	protected int calculateYOffset() {
		return (distributionRegion.getLevel() * LEVEL_DISTANCE) + panel.getRootPosY();
	}
	
	/**
	 * Is the mouse over this component?
	 * @return
	 */
	public boolean isMouseOver(final MouseEvent event) {
	
		if(event.getX() >= xOffset && event.getX() <= xOffset + WIDTH) {
			if(event.getY() >= yOffset && event.getY() <= yOffset + HEIGHT) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Draw this component
	 * @param g
	 * @return 
	 */
	public BoundingBox drawComponent(final Graphics2D g) {
		
		// Recalculate the offsets
		this.xOffset = calculateXOffset();
		this.yOffset = calculateYOffset();
		
		// Draw the node
		final Color oldColor = g.getColor();
		g.setColor(getColorForRegion(distributionRegion));
		g.fillRect(xOffset, yOffset, WIDTH, HEIGHT);
		g.setColor(oldColor);
		g.drawRect(xOffset, yOffset, WIDTH, HEIGHT);

		// Write the region id
		final String regionId = "Region: " + Long.toString(distributionRegion.getRegionId());
		writeStringCentered(g, regionId, 0.3);
		
		// Write the state
		final String nodeState = distributionRegion.getState().getStringValue();
		writeStringCentered(g, nodeState, 0.6);
		
		// Write the split position
		if(distributionRegion.isLeafRegion()) {
			writeStringCentered(g, "-", 0.9);
		} else {
			String nodeText = Double.toString(distributionRegion.getSplit());
			writeStringCentered(g, nodeText, 0.9);
		}
		
		// Draw the line to the parent node
		drawParentNodeLine(g);
		
		return getBoundingBox();				
	}

	/**
	 * Returns the bounding box of the component
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		final BoundingBox boundingBox = new BoundingBox((double) xOffset, 
				(double) xOffset + WIDTH, (double) yOffset, (double) yOffset + HEIGHT);
				
		return boundingBox;
	}

	/**
	 * Write the given string centered
	 * @param g
	 * @param nodeState
	 * @param pos
	 */
	protected void writeStringCentered(final Graphics2D g, final String nodeState, final double pos) {
		final Rectangle2D boundsState = g.getFontMetrics().getStringBounds(nodeState, g);
		int stringWidthStage = (int) boundsState.getWidth();
		g.drawString(nodeState, xOffset + (WIDTH / 2) - (stringWidthStage / 2), yOffset + (int) (HEIGHT * pos));
	}

	/**
	 * Get the color for the distribution region
	 * @param distributionRegion
	 * @return
	 */
	protected Color getColorForRegion(final DistributionRegion distributionRegion) {
		switch (distributionRegion.getState()) {
		case ACTIVE:
		case ACTIVE_FULL:
			return Color.GREEN;
			
		case SPLIT:
			return new Color(144, 144, 144);
			
		case SPLITTING:
			return Color.YELLOW;
			
		default:
			return Color.LIGHT_GRAY;
		}
	}

	/**
	 * Draw the line to the parent node
	 * @param g
	 * @param xOffset
	 * @param yOffset
	 */
	protected void drawParentNodeLine(final Graphics2D g) {
		
		// The root element don't have a parent
		if(distributionRegion.getParent() == null) {
			return;
		}
		
		int lineEndX = xOffset + (WIDTH / 2);
		int lineEndY = yOffset - LEVEL_DISTANCE + HEIGHT;
		
		if(distributionRegion.isLeftChild()) {
			lineEndX = lineEndX + calculateLevelXOffset(distributionRegion.getLevel());
		} else {
			lineEndX = lineEndX - calculateLevelXOffset(distributionRegion.getLevel());
		}
		
		g.drawLine(xOffset + (WIDTH / 2), yOffset, lineEndX, lineEndY);
	}

	/**
	 * Get the tooltip text
	 * @return
	 */
	public String getToolTipText() {
		
		final BoundingBox boundingBox = distributionRegion.getConveringBox();
		final StringBuilder sb = new StringBuilder("<html>");
		
		for(int i = 0; i < boundingBox.getDimension(); i++) {
			final DoubleInterval floatInterval = boundingBox.getIntervalForDimension(i);
			sb.append("Dimension: " + i + " ");
			sb.append(floatInterval.toString());
			sb.append("<br>");
		}
		
		final String systemsString = distributionRegion.getSystems()
				.stream()
				.map(s -> s.toGUIString())
				.collect(Collectors.joining(", "));
		
		sb.append("Systems: ");
		sb.append(systemsString);
		
		appendStatistics(sb);
		
		sb.append("</html>");
		return sb.toString();
	}

	/**
	 * Append the statistics to the tooltip
	 * @param sb
	 */
	private void appendStatistics(final StringBuilder sb) {
		try {
			final DistributionGroupZookeeperAdapter adapter = ZookeeperClientFactory.getDistributionGroupAdapter();
			final Map<BBoxDBInstance, Map<String, Long>> statistics = adapter.getRegionStatistics(distributionRegion);

			for(final BBoxDBInstance instance : statistics.keySet()) {
				final Map<String, Long> statisticData = statistics.get(instance);
				sb.append("System: ");
				sb.append(instance);
				sb.append(" ");
				sb.append(statisticData);
				sb.append("<br>");
			}
		} catch (Exception e) {
			logger.error("Got an exception while reading statistics for distribution group", e);
		} 
	}

	/**
	 * Return the distribution region
	 * @return
	 */
	public DistributionRegion getDistributionRegion() {
		return distributionRegion;
	}
}

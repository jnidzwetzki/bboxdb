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
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.event.MouseEvent;
import java.awt.geom.Rectangle2D;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.regionsplit.RegionMergeHelper;
import org.bboxdb.distribution.partitioner.regionsplit.RegionSplitHelper;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNodeNames;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.tools.gui.views.TreeJPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionRegionComponent {
	
	/**
	 * The padding between two nodes
	 */
	public final static int PADDING_LEFT_RIGHT = 15;
	
	/**
	 * The padding between two levels
	 */
	public final static int LEVEL_DISTANCE = 100;

	/**
	 * The height of the box
	 */
	public final int HEIGHT;
	
	/**
	 * The width of the box
	 */
	public final static int WIDTH = 110;
	
	/**
	 * The distribution region to paint
	 */
	private final DistributionRegion distributionRegion;

	/**
	 * The x offset
	 */
	private int xOffset;

	/**
	 * The y offset
	 */
	private int yOffset;
	
	/**
	 * The panel
	 */
	private final TreeJPanel panel;
	
	/**
	 * The GUI model
	 */
	private GuiModel guiModel;

	/**
	 * The max number of children
	 */
	private int maxChildren;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ConnectDialog.class);


	public DistributionRegionComponent(final DistributionRegion distributionRegion, 
			final TreeJPanel distributionGroupJPanel, final GuiModel guiModel, final int maxChildren) {
		
		this.distributionRegion = distributionRegion;
		this.panel = distributionGroupJPanel;
		this.guiModel = guiModel;
		this.maxChildren = maxChildren;

		this.xOffset = calculateXOffset(distributionRegion);
		this.yOffset = calculateYOffset();
		
		this.HEIGHT = 40 + (distributionRegion.getConveringBox().getDimension() * 15);
	}
	
	/**
	 * Calculate the x offset of the component
	 * @param maxChildren 
	 * @return
	 */
	private int calculateXOffset(DistributionRegion region) {
		int offset = panel.getRootPosX();
		
		
		while(! region.isRootElement()) {
			
			final List<DistributionRegion> children = region.getParent().getDirectChildren();
			
			final int noOfChildren = children.size();
			final int silbingsBlockSize = (noOfChildren + 1) * levelChildrenDistance(region.getLevel());
			
			offset = offset - (silbingsBlockSize / 2);
			
			for(int i = 0; i < children.size(); i++) {
				offset = offset + levelChildrenDistance(region.getLevel());

				if(children.get(i) == region) {
					break;
				}
			}
			
			region = region.getParent();
		}
		
		return offset;
	}
	
	/**
	 * Calculate the x offset for a given level
	 * @param level
	 * @return
	 */
	private int levelChildrenDistance(final int level) {
		final int offsetLastLevel = (PADDING_LEFT_RIGHT + WIDTH);
		
		final int curentLevel = distributionRegion.getTotalLevel() - level - 1;
		
		return (int) (offsetLastLevel * Math.pow(maxChildren, curentLevel));
	}
	

	/**
	 * Draw the line to the parent node
	 * @param g
	 * @param xOffset
	 * @param yOffset
	 */
	private void drawParentNodeLine(final Graphics2D g) {
		
		// The root element don't have a parent
		if(distributionRegion.isRootElement()) {
			return;
		}
		
		final int parentOffset = calculateXOffset(distributionRegion.getParent());

		int lineEndX = parentOffset + (WIDTH / 2);
		int lineEndY = yOffset - LEVEL_DISTANCE + HEIGHT;
		
		g.drawLine(xOffset + (WIDTH / 2), yOffset, lineEndX, lineEndY);
	}

	/**
	 * Calculate the Y offset
	 * @return
	 */
	private int calculateYOffset() {
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
	 * @param maxChildren 
	 * @return 
	 */
	public Hyperrectangle drawComponent(final Graphics2D g) {
		
		// Recalculate the offsets
		this.xOffset = calculateXOffset(distributionRegion);
		this.yOffset = calculateYOffset();
		
		// Draw the node
		final Color oldColor = g.getColor();
		g.setColor(getColorForRegion(distributionRegion));
		g.fillRect(xOffset, yOffset, WIDTH, HEIGHT);
		g.setColor(oldColor);
		g.drawRect(xOffset, yOffset, WIDTH, HEIGHT);

		final Hyperrectangle converingBox = distributionRegion.getConveringBox();

		final double offset = (double) 0.9 / (double) (2.0 + converingBox.getDimension());
		
		// Write the region id
		final String regionId = "Region: " + Long.toString(distributionRegion.getRegionId());
		writeStringCentered(g, regionId, 1 * offset);
		
		// Write the state
		final String nodeState = distributionRegion.getState().getStringValue();
		writeStringCentered(g, nodeState, 2 * offset);
		
		// Write the bounding box
		for(int i = 0; i < converingBox.getDimension(); i++) {
			final DoubleInterval floatInterval = converingBox.getIntervalForDimension(i);
			final String text = "D" + i + ": " + floatInterval.getRoundedString(3);
			writeStringCentered(g, text, (3+i) * offset);
		}
		
		// Draw the line to the parent node
		drawParentNodeLine(g);
		
		return getBoundingBox();				
	}

	/**
	 * Returns the bounding box of the component
	 * @return
	 */
	public Hyperrectangle getBoundingBox() {
		final Hyperrectangle boundingBox = new Hyperrectangle((double) xOffset, 
				(double) xOffset + WIDTH, (double) yOffset, (double) yOffset + HEIGHT);
				
		return boundingBox;
	}

	/**
	 * Write the given string centered
	 * @param g
	 * @param stringValue
	 * @param pos
	 * @return 
	 */
	private void writeStringCentered(final Graphics2D g, final String stringValue, final double pos) {
		final int stringWidth = calculateStringWidth(g, stringValue);
		final int centerValue = xOffset + (WIDTH / 2) - (stringWidth / 2);
		final Font font = g.getFont();
		g.setFont(new Font(font.getFontName(), font.getStyle(), 10)); 
		g.drawString(stringValue, centerValue, yOffset + (int) (HEIGHT * pos));
	}

	/**
	 * Calculate the string width
	 * @param g
	 * @param stringValue
	 * @return
	 */
	private int calculateStringWidth(final Graphics2D g, final String stringValue) {
		final Rectangle2D boundsState = g.getFontMetrics().getStringBounds(stringValue, g);
		return (int) boundsState.getWidth();
	}

	/**
	 * Get the color for the distribution region
	 * @param distributionRegion
	 * @return
	 */
	private Color getColorForRegion(final DistributionRegion distributionRegion) {
		switch (distributionRegion.getState()) {
		case ACTIVE:
		case ACTIVE_FULL:
		case REDISTRIBUTION_ACTIVE:
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
	 * Get the tooltip text
	 * @return
	 */
	public String getToolTipText() {		
		final StringBuilder sb = new StringBuilder("<html>");
	
		try {
			final Map<BBoxDBInstance, Map<String, Long>> statistics = addStatisticsToTooltip(sb);
			
			final Hyperrectangle boundingBox = distributionRegion.getConveringBox();
			for(int i = 0; i < boundingBox.getDimension(); i++) {
				final DoubleInterval floatInterval = boundingBox.getIntervalForDimension(i);
				sb.append("Dimension: " + i + " ");
				sb.append(floatInterval.toString());
				sb.append("<br>");
			}
			
			final Collection<BBoxDBInstance> systems = distributionRegion.getSystems();
			for(final BBoxDBInstance instance : systems) {
				if(! statistics.keySet().contains(instance)) {
					sb.append("System: ");
					sb.append(instance.toGUIString(guiModel.isScreenshotMode()));
					sb.append(" <br>");
				}
			}
			
			final boolean mergeableByZookeeper 
				= RegionMergeHelper.isMergingByZookeeperAllowed(distributionRegion);
			
			final boolean mergeableBySpacePartitioner
				= RegionMergeHelper.isMergingBySpacePartitionerAllowed(distributionRegion);
			
			sb.append("Merge supported by configuration <i>" + mergeableByZookeeper 
					+ "</i>, by space partitioner <i>" + mergeableBySpacePartitioner + "</i><br>");
			
			final boolean isSplitSupported = RegionSplitHelper.isSplittingSupported(distributionRegion);
			
			sb.append("Split supported by space partitioner <i>" + isSplitSupported + "</i><br>");
		} catch (Exception e) {
			logger.error("Got an exception while reading statistics for distribution group", e);
		} 
		
		sb.append("</html>");
		return sb.toString();
	}

	/**
	 * Add the statistics to the tooltip
	 * @param sb
	 * @return
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private Map<BBoxDBInstance, Map<String, Long>> addStatisticsToTooltip(final StringBuilder sb)
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final DistributionRegionAdapter adapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
		
		final Map<BBoxDBInstance, Map<String, Long>> statistics 
			= adapter.getRegionStatistics(distributionRegion);

		for(final BBoxDBInstance instance : statistics.keySet()) {
			final Map<String, Long> statisticData = statistics.get(instance);
			sb.append("Node: ");
			sb.append(instance.toGUIString(guiModel.isScreenshotMode()));
			sb.append(" Tuples: ");
			sb.append(statisticData.get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_TUPLES));
			sb.append(", Size: ");
			sb.append(statisticData.get(ZookeeperNodeNames.NAME_STATISTICS_TOTAL_SIZE));
			sb.append(" MB <br>");
		}
		
		return statistics;
	}

	/**
	 * Return the distribution region
	 * @return
	 */
	public DistributionRegion getDistributionRegion() {
		return distributionRegion;
	}
}

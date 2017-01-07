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

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JPanel;

import org.bboxdb.distribution.DistributionRegion;

public class DistributionGroupJPanel extends JPanel {
	
	private static final long serialVersionUID = -248493308846818192L;
	
	/**
	 * The regions
	 */
	protected final List<DistributionRegionComponent> regions = new ArrayList<DistributionRegionComponent>();
	
	/**
	 * The curent size of the component
	 */
	protected Dimension componentSize;
	
	/**
	 * The GUI model
	 */
	protected final GuiModel guiModel;

	public DistributionGroupJPanel(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}

	/**
	 * Draw the given distribution region
	 * @param graphics2d
	 * @param distributionRegion
	 */
	protected void drawDistributionRegion(final Graphics2D graphics2d, final DistributionRegion distributionRegion) {

		if(distributionRegion == null) {
			return;
		}

		// The position of the root node
		final int rootPosX = getWidth() / 2;
		final int rootPosY = 30;

		final DistributionRegionComponent distributionRegionComponent = new DistributionRegionComponent(distributionRegion, rootPosX, rootPosY);
		distributionRegionComponent.drawComponent(graphics2d);
		regions.add(distributionRegionComponent);

		drawDistributionRegion(graphics2d, distributionRegion.getLeftChild());
		drawDistributionRegion(graphics2d, distributionRegion.getRightChild());
	}

	@Override
	protected void paintComponent(final Graphics g) {
		super.paintComponent(g);

		final Graphics2D graphics2D = (Graphics2D) g;
		graphics2D.setRenderingHint(
				RenderingHints.KEY_ANTIALIASING, 
				RenderingHints.VALUE_ANTIALIAS_ON);

		// Group is not set
		if(guiModel.getTreeAdapter() == null) {
			return;
		}

		final DistributionRegion distributionRegion = guiModel.getTreeAdapter().getRootNode();

		regions.clear();
		drawDistributionRegion(graphics2D, distributionRegion);

		g.drawString("Cluster name: " + guiModel.getClustername(), 10, 20);
		g.drawString("Distribution group: " + guiModel.getDistributionGroup(), 10, 40);
		g.drawString("Replication factor: " + guiModel.getReplicationFactor(), 10, 60);

		updateComponentSize(distributionRegion);
	}

	/**
	 * Update the size of the component
	 * @param distributionRegion
	 */
	protected void updateComponentSize(final DistributionRegion distributionRegion) {
		final int totalLevel = distributionRegion.getTotalLevel();

		final int totalWidth =  
				+ ((DistributionRegionComponent.LEFT_RIGHT_OFFSET 
						+ DistributionRegionComponent.WIDTH) * totalLevel) * 2;
		final int totalHeight = DistributionRegionComponent.HEIGHT 
				+ (totalLevel * DistributionRegionComponent.LEVEL_DISTANCE);

		final Dimension curentSize = new Dimension(totalWidth, totalHeight);

		// Size has changed, update
		if(! curentSize.equals(componentSize)) {
			componentSize = curentSize;
			setPreferredSize(curentSize);
			setSize(curentSize);
		}
	}

	/**
	 * Get the text for the tool tip
	 */
	@Override
	public String getToolTipText(final MouseEvent event) {

		for(final DistributionRegionComponent component : regions) {
			if(component.isMouseOver(event)) {
				return component.getToolTipText();
			}
		}

		return "";
	}
}
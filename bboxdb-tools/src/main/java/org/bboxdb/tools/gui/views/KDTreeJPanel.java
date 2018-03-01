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
package org.bboxdb.tools.gui.views;

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JPanel;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.tools.gui.DistributionRegionComponent;
import org.bboxdb.tools.gui.GuiModel;

public class KDTreeJPanel extends JPanel {
	
	private static final long serialVersionUID = -248493308846818192L;
	
	/**
	 * The regions
	 */
	protected final List<DistributionRegionComponent> regions = new ArrayList<>();
	
	/**
	 * The current size of the component
	 */
	protected Dimension componentSize;
	
	/**
	 * The GUI model
	 */
	protected final GuiModel guiModel;
	
	/**
	 * The margin for bounding boxes 
	 * (adds a little bit empty space around the components)
	 */
	protected static final int PADDING_TOP_BOTTOM = 10;

	/**
	 * The padding on the left and right side
	 */
	protected static final int PADDING_LEFT_RIGHT = 40;
	
	/**
	 * The x-position of the root-node
	 */
	protected int rootPosX;

	/**
	 * The y-position of the root-node
	 */
	protected final int rootPosY;

	public KDTreeJPanel(final GuiModel guiModel) {
		this.guiModel = guiModel;
		this.rootPosX = 1000;
		this.rootPosY = 30;
	}

	/**
	 * Draw the given distribution region
	 * @param graphics2d
	 * @param distributionRegion
	 * @return 
	 */
	protected BoundingBox drawDistributionRegion(final Graphics2D graphics2d) {

		BoundingBox minBoundingBox = BoundingBox.FULL_SPACE;
		
		for(DistributionRegionComponent component : regions) {
			final BoundingBox boundingBox = component.drawComponent(graphics2d);
			minBoundingBox = BoundingBox.getCoveringBox(boundingBox, minBoundingBox);
		}
		
		return minBoundingBox;
	}

	/**
	 * Get the minimal bounding box for this component
	 * @return
	 */
	protected BoundingBox getMinimalBoundingBox() {
		return new BoundingBox(0.0, 400.0, 0.0, 200.0);
	}
	
	/**
	 * Create the distribution region components
	 * @param distributionRegion
	 */
	protected void createDistribtionRegionComponents(final DistributionRegion distributionRegion) {
		
		if(distributionRegion == null) {
			return;
		}
		
		final DistributionRegionComponent distributionRegionComponent = new DistributionRegionComponent(distributionRegion, this);
		regions.add(distributionRegionComponent);
				
		for(final DistributionRegion region : distributionRegion.getDirectChildren()) {
			createDistribtionRegionComponents(region);
		}
	}
	
	@Override
	protected void paintComponent(final Graphics g) {
		super.paintComponent(g);
		
		// Group is not set
		if(guiModel.getTreeAdapter() == null) {
			return;
		}

		final Graphics2D graphics2D = (Graphics2D) g;
		graphics2D.setRenderingHint(
				RenderingHints.KEY_ANTIALIASING, 
				RenderingHints.VALUE_ANTIALIAS_ON);
		

		final DistributionRegion distributionRegion = guiModel.getTreeAdapter().getRootNode();
		
		regions.clear();
		
		// Fake position to calculate the real width
		rootPosX = 100000;
		createDistribtionRegionComponents(distributionRegion);
		
		calculateRootNodeXPos();
		
		final BoundingBox drawBox = drawDistributionRegion(graphics2D);
	
		updateComponentSize(drawBox);
	}

	/**
	 * Calculate the root node X postion
	 */
	protected void calculateRootNodeXPos() {
		
		if(regions.isEmpty()) {
			return;
		}
		
		BoundingBox minBoundingBox = BoundingBox.FULL_SPACE;
		
		for(DistributionRegionComponent component : regions) {
			minBoundingBox = BoundingBox.getCoveringBox(component.getBoundingBox(), minBoundingBox);
		}
						
		final double rootCoordinateLow = regions.get(0).getBoundingBox().getCoordinateLow(0);
		final double bboxCoordinateLow = minBoundingBox.getCoordinateLow(0);
		
		final int rootOffsetBBox = (int) (rootCoordinateLow - bboxCoordinateLow) + PADDING_LEFT_RIGHT;

		final int rootOffsetWindowSize = (int) ((getVisibleRect().getWidth() - DistributionRegionComponent.WIDTH) / 2);
		
		// If the screen is bigger as the bounding box, use the screen width
		rootPosX = Math.max(rootOffsetBBox, rootOffsetWindowSize);	
	}

	/**
	 * Update the size of the component
	 * @param distributionRegion
	 * @param boundingBox 
	 */
	protected void updateComponentSize(final BoundingBox boundingBox) {
		
		if(boundingBox == BoundingBox.FULL_SPACE) {
			return;
		}

		final Dimension boundingBoxSize = new Dimension(
				(int) boundingBox.getExtent(0) + (2 * PADDING_LEFT_RIGHT), 
				(int) boundingBox.getExtent(1) + (2 * PADDING_TOP_BOTTOM) + rootPosY);

		// Size has changed, update
		if(! boundingBoxSize.equals(componentSize)) {
			componentSize = boundingBoxSize;
			setPreferredSize(boundingBoxSize);
			setSize(boundingBoxSize);
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
		
        setToolTipText(null);
		return super.getToolTipText(event);
	}

	/**
	 * Get the x pos of the root node
	 * @return
	 */
	public int getRootPosX() {
		return rootPosX;
	}

	/**
	 * Get the y pos of the root node
	 * @return
	 */
	public int getRootPosY() {
		return rootPosY;
	}
}
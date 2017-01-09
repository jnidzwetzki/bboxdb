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
import org.bboxdb.storage.entity.BoundingBox;

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
	
	/**
	 * The margin for bounding boxes 
	 * (adds a litte bit empty space around the components)
	 */
	protected static final int SCROLL_MARGIN = 10;


	public DistributionGroupJPanel(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}

	/**
	 * Draw the given distribution region
	 * @param graphics2d
	 * @param distributionRegion
	 * @return 
	 */
	protected BoundingBox drawDistributionRegion(final Graphics2D graphics2d, 
			final DistributionRegion distributionRegion) {

		if(distributionRegion == null) {
			return BoundingBox.EMPTY_BOX;
		}

		// The position of the root node
		final int rootPosX = getWidth() / 2;
		final int rootPosY = 30;

		final DistributionRegionComponent distributionRegionComponent = new DistributionRegionComponent(distributionRegion, rootPosX, rootPosY);
		final BoundingBox boundingBox = distributionRegionComponent.drawComponent(graphics2d);
		regions.add(distributionRegionComponent);

		final BoundingBox boundingBoxLeft 
			= drawDistributionRegion(graphics2d, distributionRegion.getLeftChild());
		final BoundingBox boundingBoxRight 
			= drawDistributionRegion(graphics2d, distributionRegion.getRightChild());
		
		return BoundingBox.getCoveringBox(boundingBox, boundingBoxLeft, boundingBoxRight);
	}

	@Override
	protected void paintComponent(final Graphics g) {
		super.paintComponent(g);

		final Graphics2D graphics2D = (Graphics2D) g;
		graphics2D.setRenderingHint(
				RenderingHints.KEY_ANTIALIASING, 
				RenderingHints.VALUE_ANTIALIAS_ON);
		
		regions.clear();

		// Group is not set
		if(guiModel.getTreeAdapter() == null) {
			return;
		}
		
		final DistributionRegion distributionRegion = guiModel.getTreeAdapter().getRootNode();

		final BoundingBox boundingBoxRegion = drawDistributionRegion(graphics2D, distributionRegion);
		final BoundingBox baseDimension = new BoundingBox(0f, 400f, 0f, 200f);
		final BoundingBox boundingBoxTotal = BoundingBox.getCoveringBox(boundingBoxRegion, baseDimension);
		
		updateComponentSize(boundingBoxTotal);
	}

	/**
	 * Update the size of the component
	 * @param distributionRegion
	 * @param boundingBox 
	 */
	protected void updateComponentSize(final BoundingBox boundingBox) {

		final Dimension boundingBoxSize = new Dimension(
				(int) boundingBox.getExtent(0) + SCROLL_MARGIN, 
				(int) boundingBox.getExtent(1) + SCROLL_MARGIN);

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

		return "";
	}
}
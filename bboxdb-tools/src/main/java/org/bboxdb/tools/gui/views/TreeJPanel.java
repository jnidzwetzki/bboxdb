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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JPanel;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.tools.gui.DistributionRegionComponent;
import org.bboxdb.tools.gui.GuiModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeJPanel extends JPanel {
	
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
	private static final int PADDING_TOP_BOTTOM = 10;

	/**
	 * The padding on the left and right side
	 */
	private static final int PADDING_LEFT_RIGHT = 40;
	
	/**
	 * The x-position of the root-node
	 */
	protected int rootPosX;

	/**
	 * The y-position of the root-node
	 */
	protected final int rootPosY;
	
	/**
	 * The zoom factor
	 */
	private double zoomFactor;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TreeJPanel.class);

	public TreeJPanel(final GuiModel guiModel) {
		this.guiModel = guiModel;
		this.rootPosX = 1000;
		this.rootPosY = 30;
		this.zoomFactor = 1.0;
		setToolTipText("");
	}

	/**
	 * Draw the given distribution region
	 * @param graphics2d
	 * @param maxChildren 
	 * @param distributionRegion
	 * @return 
	 */
	protected Hyperrectangle drawDistributionRegion(final Graphics2D graphics2d) {

		Hyperrectangle minBoundingBox = Hyperrectangle.FULL_SPACE;
		
		for(final DistributionRegionComponent component : regions) {
			final Hyperrectangle boundingBox = component.drawComponent(graphics2d);
			minBoundingBox = Hyperrectangle.getCoveringBox(boundingBox, minBoundingBox);
		}
		
		return minBoundingBox;
	}
	
	/**
	 * Create the distribution region components
	 * @param distributionRegion
	 * @param maxChildren 
	 */
	protected void createDistribtionRegionComponents(final DistributionRegion distributionRegion, 
			final int maxChildren) {
		
		if(distributionRegion == null) {
			return;
		}
		
		final DistributionRegionComponent distributionRegionComponent 
			= new DistributionRegionComponent(distributionRegion, this, guiModel, maxChildren);
		
		regions.add(distributionRegionComponent);
				
		for(final DistributionRegion region : distributionRegion.getDirectChildren()) {
			createDistribtionRegionComponents(region, maxChildren);
		}
	}
	
	@Override
	protected void paintComponent(final Graphics g) {
		super.paintComponent(g);
		
		setBackground(Color.WHITE);
		
		// Group is not set
		if(guiModel.getTreeAdapter() == null) {
			return;
		}

		final Graphics2D graphics2D = (Graphics2D) g;
		graphics2D.setRenderingHint(
				RenderingHints.KEY_ANTIALIASING, 
				RenderingHints.VALUE_ANTIALIAS_ON);
		
		graphics2D.scale(zoomFactor, zoomFactor);		
		
		try {
			final DistributionRegion distributionRegion = guiModel.getTreeAdapter().getRootNode();
			
			regions.clear();
			
			if(distributionRegion == null) {
				logger.error("Got null root node");
				return;
			}
			
			final int maxChildren = distributionRegion
					.getThisAndChildRegions()
					.stream()
					.mapToInt(r -> r.getDirectChildren().size())
					.max()
					.orElse(0);
			
			// Fake position to calculate the real width
			rootPosX = 100000;
			createDistribtionRegionComponents(distributionRegion, maxChildren);
			
			calculateRootNodeXPos();
			
			final Hyperrectangle drawBox = drawDistributionRegion(graphics2D);
			
			updateComponentSize(drawBox);
		} catch (BBoxDBException e) {
			logger.error("Got an exception", e);
		}
	}

	/**
	 * Calculate the root node X position
	 */
	protected void calculateRootNodeXPos() {
		
		if(regions.isEmpty()) {
			return;
		}
		
		Hyperrectangle minBoundingBox = Hyperrectangle.FULL_SPACE;
		
		for(DistributionRegionComponent component : regions) {
			minBoundingBox = Hyperrectangle.getCoveringBox(component.getBoundingBox(), minBoundingBox);
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
	protected void updateComponentSize(final Hyperrectangle boundingBox) {
		
		if(boundingBox == Hyperrectangle.FULL_SPACE) {
			return;
		}
				
		final int preferedWidth = (int) ((boundingBox.getCoordinateHigh(0) + (2 * PADDING_LEFT_RIGHT)) * zoomFactor);
		final int preferedHeight = (int) ((boundingBox.getCoordinateHigh(1) + (2 * PADDING_TOP_BOTTOM) + rootPosY) * zoomFactor);
		
		final Dimension boundingBoxSize = new Dimension(
				preferedWidth, 
				preferedHeight);

		// Size has changed, update
		if(! boundingBoxSize.equals(componentSize)) {
			componentSize = boundingBoxSize;
			
			setPreferredSize(boundingBoxSize);
			setMinimumSize(boundingBoxSize);
			
			revalidate();
			repaint();
		}
	}
	
	/**
	 * Get the text for the tool tip
	 */
	@Override
	public String getToolTipText(final MouseEvent event) {

		final MouseEvent scaledEvent = new MouseEvent(event.getComponent(), 
				event.getID(), 
				event.getWhen(), 
				event.getModifiers(), 
				(int) (event.getX() / zoomFactor), 
				(int) (event.getY() / zoomFactor), 
				event.getClickCount(), 
				event.isPopupTrigger());

		for(final DistributionRegionComponent component : regions) {
			if(component.isMouseOver(scaledEvent)) {
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
	
	/**
	 * Get the zoom factor
	 * @return
	 */
	public double getZoomFactor() {
		return zoomFactor;
	}
	
	/**
	 * Set the zoom factor
	 * @param zoomFactor
	 */
	public void setZoomFactor(final double zoomFactor) {
		this.zoomFactor = zoomFactor;
		revalidate();
		repaint();
	}
}
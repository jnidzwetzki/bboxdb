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
package org.bboxdb.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;

public class DistributionRegion {

	/**
	 * The name of the distribution group
	 */
	private final DistributionGroupName distributionGroupName;
	
	/**
	 * The split position
	 */
	private double split = Double.MIN_VALUE;
	
	/**
	 * The left child
	 */
	private final List<DistributionRegion> children;
		
	/**
	 * The parent of this node
	 */
	private final DistributionRegion parent;

	/**
	 * The area that is covered
	 */
	private BoundingBox converingBox;
	
	/**
	 * The state of the region
	 */
	private DistributionRegionState state = DistributionRegionState.UNKNOWN;
	
	/**
	 * The systems
	 */
	private Collection<BBoxDBInstance> systems;
	
	/**
	 * The id of the region
	 */
	private volatile long regionid = INVALID_REGION_ID;

	/**
	 * The invalid value for the region id
	 */
	public final static int INVALID_REGION_ID = -1;
	
	/**
	 * The root pointer of the root element of the tree
	 */
	public final static DistributionRegion ROOT_NODE_ROOT_POINTER = null;

	/**
	 * Protected constructor, the factory method and the set split methods should
	 * be used to create a tree
	 * @param name
	 * @param level
	 */
	protected DistributionRegion(final DistributionGroupName name, final DistributionRegion parent) {
		this.distributionGroupName = name;
		this.parent = parent;
		this.converingBox = BoundingBox.createFullCoveringDimensionBoundingBox(getDimension());
		this.systems = new ArrayList<>();
		this.children = new ArrayList<>();
	}

	/**
	 * Get the parent of the node
	 * @return
	 */
	public DistributionRegion getParent() {
		return parent;
	}

	/**
	 * Get the children of the region
	 * @return
	 */
	public List<DistributionRegion> getDirectChildren() {
		return new ArrayList<>(children);
	}
	
	/**
	 * Get all the children of the region
	 * @return
	 */
	public List<DistributionRegion> getAllChildren() {
		final List<DistributionRegion> result = new ArrayList<>();
		
		for(final DistributionRegion child : children) {
			result.add(child);
			result.addAll(child.getAllChildren());
		}
		
		return result;
	}
	
	/**
	 * Get all distribution regions
	 * @return
	 */
	public List<DistributionRegion> getThisAndChildRegions() {
		final List<DistributionRegion> result = new ArrayList<>();
		result.add(this);
		result.addAll(getAllChildren());
		return result;
	}
	
	/**
	 * Set the split coordinate
	 * @param split
	 */
	public void setSplit(final double split) {
		this.split = split;
		
		if(hasChilds()) {
			throw new IllegalArgumentException("Split called, but left or right node are not empty");
		}
		
		final DistributionRegion leftChild = new DistributionRegion(distributionGroupName, this);
		final DistributionRegion rightChild = new DistributionRegion(distributionGroupName, this);

		// Calculate the covering bounding boxes
		leftChild.setConveringBox(converingBox.splitAndGetLeft(split, getSplitDimension(), true));
		rightChild.setConveringBox(converingBox.splitAndGetRight(split, getSplitDimension(), false));
		
		
		assert (children.isEmpty()) : "Children list is not empty";
		
		children.add(leftChild);
		children.add(rightChild);
	}
	
	/**
	 * Set the childs to state active
	 */
	public void makeChildsActive() {
		for(final DistributionRegion child : children) {
			child.setState(DistributionRegionState.ACTIVE);
		}
	}

	/**
	 * Merge the distribution group
	 */
	public void merge() {
		split = Double.MIN_VALUE;
		children.clear();
	}
	
	/**
	 * Get the split coordinate
	 * @return
	 */
	public double getSplit() {
		return split;
	}
	
	/**
	 * Get the state of the node
	 * @return
	 */
	public DistributionRegionState getState() {
		return state;
	}

	/**
	 * Set the state of the node
	 * @param state
	 */
	public void setState(final DistributionRegionState state) {
		this.state = state;
	}

	/**
	 * Get the level of the region
	 * @return
	 */
	public int getLevel() {
		int levelCounter = 0;
		
		DistributionRegion parent = this.getParent();
		
		while(parent != null) {
			parent = parent.getParent();
			levelCounter++;
		}
		
		return levelCounter;
	}
	
	/**
	 * Get the number of levels in this tree
	 * @return
	 */
	public int getTotalLevel() {		
		return getRootRegion().getAllChildren()
				.stream()
				.mapToInt(d -> d.getLevel())
				.max()
				.orElse(0) + 1;
	}

	/**
	 * Get the dimension of the distribution region
	 * @return
	 */
	public int getDimension() {
		try {
			final DistributionGroupConfigurationCache instance = DistributionGroupConfigurationCache.getInstance();
			final DistributionGroupConfiguration config = instance.getDistributionGroupConfiguration(distributionGroupName);
			return config.getDimensions();
		} catch (ZookeeperNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Returns the dimension of the split
	 * @return
	 */
	public int getSplitDimension() {
		return getLevel() % getDimension();
	}

	@Override
	public String toString() {
		return "DistributionRegion [distributionGroupName=" + distributionGroupName + ", "
				+ ", split=" + split + ", converingBox=" + converingBox + ", state=" + state
				+ ", systems=" + systems + ", nameprefix=" + regionid + "]";
	}

	/**
	 * Get the root element of the region
	 */
	public DistributionRegion getRootRegion() {
		DistributionRegion currentElement = this;
		
		// Follow the links to the root element
		while(currentElement.parent != ROOT_NODE_ROOT_POINTER) {
			currentElement = currentElement.parent;
		}
		
		return currentElement;
	}
	
	/**
	 * Is this a leaf region node?
	 * @return
	 */
	public boolean isLeafRegion() {
		
		for(final DistributionRegion child : children) {
			if(child.getState() != DistributionRegionState.CREATING 
					&& child.getState() != DistributionRegionState.UNKNOWN) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Has this node childs?
	 * @return
	 */
	public boolean hasChilds() {
		return ! children.isEmpty();
	}
	
	/**
	 * Remove the children
	 */
	public void removeChildren() {
		children.clear();
	}
	
	/**
	 * Child nodes are in creation?
	 * @return
	 */
	public boolean isChildNodesInCreatingState() {
		return children.stream()
			.anyMatch(r -> r.getState() == DistributionRegionState.CREATING);
	}
	
	/**
	 * Is this the root element?
	 */
	public boolean isRootElement() {
		return (parent == ROOT_NODE_ROOT_POINTER);
	}
	
	/**
	 * Is this the left child of the parent
	 * @return
	 */
	public int getChildNumber() {
		
		// This is the root element
		if(isRootElement()) {
			return 0;
		}
		
		int childNumber = 0;
		
		for(final DistributionRegion region : getParent().getDirectChildren()) {
			if(region == this) {
				return childNumber;
			}
			
			childNumber++;
		}
		
		throw new RuntimeException("Unable to find child number for: " + this);
	}

	/**
	 * Get the covering bounding box
	 * @return
	 */
	public BoundingBox getConveringBox() {
		return converingBox;
	}

	/**
	 * Set the covering bounding box
	 * @param converingBox
	 */
	public void setConveringBox(final BoundingBox converingBox) {
		this.converingBox = converingBox;
	}
	
	/**
	 * Returns get the distribution group name
	 * @return
	 */
	public DistributionGroupName getDistributionGroupName() {
		return distributionGroupName;
	}
	
	/**
	 * Get a unique identifier for this region
	 * @return
	 */
	public String getIdentifier() {
		return distributionGroupName.getFullname() + "_" + regionid;
	}
	
	/**
	 * Get all systems that are responsible for this DistributionRegion
	 * @return
	 */
	public List<BBoxDBInstance> getSystems() {
		return new ArrayList<BBoxDBInstance>(systems);
	}
	
	/**
	 * Add a system to this DistributionRegion
	 * @param system
	 */
	public void addSystem(final BBoxDBInstance system) {
		systems.add(system);
	}
	
	/**
	 * Set the systems for this DistributionRegion
	 * @param newSystems
	 */
	public void setSystems(final Collection<BBoxDBInstance> newSystems) {
		
		if(newSystems == null || newSystems.isEmpty()) {
			this.systems.clear();
			return;
		}
		
		final ArrayList<BBoxDBInstance> newSystemsList 
			= new ArrayList<BBoxDBInstance>(newSystems.size());
		
		newSystemsList.addAll(newSystems);
		
		this.systems = newSystemsList;
	}
	
	/**
	 * Get the DistributionRegions for a given bounding box 
	 * @param boundingBox
	 * @return
	 */
	public Set<DistributionRegion> getDistributionRegionsForBoundingBox(final BoundingBox boundingBox) {
		return getThisAndChildRegions().stream()
			.filter(r -> r.getConveringBox().overlaps(boundingBox))
			.collect(Collectors.toSet());
	}
	
	/**
	 * Get the region id of the node
	 * @return
	 */
	public long getRegionId() {
		return regionid;
	}

	/**
	 * Set a new region id
	 * @param regionid
	 */
	public void setRegionId(final int regionid) {
		this.regionid = regionid;
	}
	
	/**
	 * Create a new root element
	 * @param distributionGroup
	 * @return
	 */
	public static DistributionRegion createRootElement(final DistributionGroupName distributionGroupName) {
		final DistributionRegion rootElement = new DistributionRegion(distributionGroupName, 
				ROOT_NODE_ROOT_POINTER);
		
		return rootElement;
	}

}


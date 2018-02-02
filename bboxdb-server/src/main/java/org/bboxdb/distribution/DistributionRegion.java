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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;

public class DistributionRegion {

	/**
	 * The name of the distribution group
	 */
	protected final DistributionGroupName distributionGroupName;
	
	/**
	 * A pointer that points to the total number of levels
	 */
	protected final AtomicInteger totalLevel;
	
	/**
	 * The split position
	 */
	protected double split = Double.MIN_VALUE;
	
	/**
	 * The left child
	 */
	protected DistributionRegion leftChild = null;
	
	/**
	 * The right child
	 */
	protected DistributionRegion rightChild = null;
	
	/**
	 * The parent of this node
	 */
	protected final DistributionRegion parent;
	
	/**
	 * The level of the node
	 * @param distributionGroupName
	 */
	protected final int level;
	
	/**
	 * The area that is covered
	 */
	protected BoundingBox converingBox;
	
	/**
	 * The state of the region
	 */
	protected DistributionRegionState state = DistributionRegionState.UNKNOWN;
	
	/**
	 * The systems
	 */
	protected Collection<BBoxDBInstance> systems;
	
	/**
	 * The id of the region
	 */
	protected volatile long regionid = INVALID_REGION_ID;

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
		
		// The level for the root node is 0
		if(parent == ROOT_NODE_ROOT_POINTER) {
			this.level = 0;
			this.totalLevel = new AtomicInteger(0);
		} else {
			this.level = parent.getLevel() + 1;
			this.totalLevel = parent.getTotalLevelPointer();
		}
		
		// Update the total amount of levels
		totalLevel.set(Math.max(totalLevel.get(), level + 1));
		
		systems = new ArrayList<BBoxDBInstance>();
	}
	
	/**
	 * Get the left child
	 * @return
	 */
	public DistributionRegion getLeftChild() {
		return leftChild;
	}

	/**
	 * Get the right child
	 * @return
	 */
	public DistributionRegion getRightChild() {
		return rightChild;
	}

	/**
	 * Get the parent of the node
	 * @return
	 */
	public DistributionRegion getParent() {
		return parent;
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
		
		leftChild = new DistributionRegion(distributionGroupName, this);
		rightChild = new DistributionRegion(distributionGroupName, this);

		// Calculate the covering bounding boxes
		leftChild.setConveringBox(converingBox.splitAndGetLeft(split, getSplitDimension(), true));
		rightChild.setConveringBox(converingBox.splitAndGetRight(split, getSplitDimension(), false));
	}
	
	/**
	 * Set the childs to state active
	 */
	public void makeChildsActive() {
		if(leftChild == null || rightChild == null) {
			return;
		}
		
		leftChild.setState(DistributionRegionState.ACTIVE);
		rightChild.setState(DistributionRegionState.ACTIVE);
	}

	/**
	 * Merge the distribution group
	 */
	public void merge() {
		split = Double.MIN_VALUE;
		leftChild = null;
		rightChild = null;
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
		return level;
	}
	
	/**
	 * Get the number of levels in this tree
	 * @return
	 */
	public int getTotalLevel() {
		return totalLevel.get();
	}
	
	/**
	 * Get the total level pointer
	 * @return
	 */
	protected AtomicInteger getTotalLevelPointer() {
		return totalLevel;
	}
	
	/**
	 * Get the dimension of the distribution region
	 * @return
	 */
	public int getDimension() {
		try {
			final DistributionGroupConfiguration config = DistributionGroupConfigurationCache.getInstance().getDistributionGroupConfiguration(distributionGroupName);
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
		return level % getDimension();
	}

	@Override
	public String toString() {
		return "DistributionRegion [distributionGroupName=" + distributionGroupName + ", totalLevel=" + totalLevel
				+ ", split=" + split + ", level=" + level + ", converingBox=" + converingBox + ", state=" + state
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
		
		if(leftChild == null 
				|| leftChild.getState() == DistributionRegionState.CREATING
				|| leftChild.getState() == DistributionRegionState.UNKNOWN) {
			return true;
		}
		
		if(rightChild == null 
				|| rightChild.getState() == DistributionRegionState.CREATING
				|| rightChild.getState() == DistributionRegionState.UNKNOWN) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Has this node childs?
	 * @return
	 */
	public boolean hasChilds() {
		return (leftChild != null && rightChild != null);
	}
	
	/**
	 * Remove the children
	 */
	public void removeChildren() {
		leftChild = null;
		rightChild = null;
	}
	
	/**
	 * Child nodes are in creation?
	 * @return
	 */
	public boolean isChildNodesInCreatingState() {
		
		if(leftChild != null && leftChild.getState() == DistributionRegionState.CREATING) {
			if(rightChild != null && rightChild.getState() == DistributionRegionState.CREATING) {
				return true;
			}
		}
		
		return false;
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
	public boolean isLeftChild() {
		// This is the root element
		if(isRootElement()) {
			return false;
		}
		
		return (parent.getLeftChild() == this);
	}
	
	/**
	 * Is this the right child of the parent
	 * @return
	 */
	public boolean isRightChild() {
		if(isRootElement()) {
			return false;
		}
		
		return (parent.getRightChild() == this);
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
		return distributionGroupName.getFullname() + " namepefix: " + regionid;
	}
	
	/**
	 * Get all systems that are responsible for this DistributionRegion
	 * @return
	 */
	public Collection<BBoxDBInstance> getSystems() {
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
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public Collection<RoutingHop> getRoutingHopsForRead(final BoundingBox boundingBox) {
		final Map<InetSocketAddress, RoutingHop> result = new HashMap<>();
		
		getHopsForBoundingBoxRecursive(boundingBox, 
				DistributionRegionHelper.PREDICATE_REGIONS_FOR_READ, 
				result);
		
		return result.values();
	}
	
	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public Collection<RoutingHop> getRoutingHopsForWrite(final BoundingBox boundingBox) {
		final Map<InetSocketAddress, RoutingHop> result = new HashMap<>();
		
		getHopsForBoundingBoxRecursive(boundingBox, 
				DistributionRegionHelper.PREDICATE_REGIONS_FOR_WRITE, 
				result);
		
		return result.values();
	}
	
	/**
	 * Add the leaf nodes systems that are covered by the bounding box
	 * @param boundingBox
	 * @param systems
	 */
	protected void getHopsForBoundingBoxRecursive(final BoundingBox boundingBox, 
			final Predicate<DistributionRegionState> statePredicate,
			final Map<InetSocketAddress, RoutingHop> hops) {
		
		// This node is not covered. So, child nodes are not covered
		if(! converingBox.overlaps(boundingBox)) {
			return;
		}
		
		if(statePredicate.test(state)) {
			for(final BBoxDBInstance system : systems) {
				if(! hops.containsKey(system.getInetSocketAddress())) {
					final RoutingHop routingHop = new RoutingHop(system, new ArrayList<Long>());
					hops.put(system.getInetSocketAddress(), routingHop);
				}
				
				final RoutingHop routingHop = hops.get(system.getInetSocketAddress());
				routingHop.addRegion(regionid);
			}
			
		} 
		
		if(! isLeafRegion()) {
			leftChild.getHopsForBoundingBoxRecursive(boundingBox, statePredicate, hops);
			rightChild.getHopsForBoundingBoxRecursive(boundingBox, statePredicate, hops);
		}
	}

	/**
	 * Get the DistributionRegions for a given bounding box 
	 * @param boundingBox
	 * @return
	 */
	public Set<DistributionRegion> getDistributionRegionsForBoundingBox(final BoundingBox boundingBox) {
		final Set<DistributionRegion> resultSet = new HashSet<DistributionRegion>();
		getDistributionRegionsForBoundingBoxRecursive(boundingBox, resultSet);
		return resultSet;
	}
	
	/**
	 * Get the DistributionRegions for a given bounding box recursive 
	 * @param boundingBox
	 * @return
	 */
	protected void getDistributionRegionsForBoundingBoxRecursive(final BoundingBox boundingBox, final Set<DistributionRegion> resultSet) {
		// This node is not covered. So, edge nodes are not covered
		if(! converingBox.overlaps(boundingBox)) {
			return;
		}
		
		resultSet.add(this);
		
		if(! isLeafRegion()) {
			leftChild.getDistributionRegionsForBoundingBoxRecursive(boundingBox, resultSet);
			rightChild.getDistributionRegionsForBoundingBoxRecursive(boundingBox, resultSet);
		}
	}

	/**
	 * Visit all child nodes of the distribution region
	 * @param distributionRegionVisitor
	 */
	public void visit(final DistributionRegionVisitor distributionRegionVisitor) {
		final boolean result = distributionRegionVisitor.visitRegion(this);
		
		if(result == false) {
			return;
		}
		
		if(isLeafRegion()) {
			return;
		}
		
		leftChild.visit(distributionRegionVisitor);
		rightChild.visit(distributionRegionVisitor);
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


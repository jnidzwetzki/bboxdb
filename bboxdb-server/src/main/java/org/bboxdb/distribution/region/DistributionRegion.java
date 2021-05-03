/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.distribution.region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.storage.entity.DistributionGroupHelper;

public class DistributionRegion {

	/**
	 * The name of the distribution group
	 */
	private final String distributionGroupName;

	/**
	 * The left child
	 */
	private final Map<Long, DistributionRegion> children;

	/**
	 * The parent of this node
	 */
	private final DistributionRegion parent;

	/**
	 * The area that is covered
	 */
	private final Hyperrectangle converingBox;

	/**
	 * The state of the region
	 */
	private DistributionRegionState state = DistributionRegionState.CREATING;

	/**
	 * The systems
	 */
	private Collection<BBoxDBInstance> systems;

	/**
	 * The id of the region
	 */
	private final long regionid;

	/**
	 * The root pointer of the root element of the tree
	 */
	public final static DistributionRegion ROOT_NODE_ROOT_POINTER = null;

	public DistributionRegion(final String name, final Hyperrectangle boundingBox) {
		this(name, ROOT_NODE_ROOT_POINTER, boundingBox, 0);
	}

	/**
	 * @param name
	 * @param boundingBox
	 * @param level
	 */
	public DistributionRegion(final String name, final DistributionRegion parent,
			final Hyperrectangle boundingBox, final long regionid) {

		if(! DistributionGroupHelper.validateDistributionGroupName(name)) {
			throw new IllegalArgumentException("Invalid distribution group specified");
		}

		this.distributionGroupName = name;
		this.converingBox = boundingBox;
		this.parent = parent;
		this.regionid = regionid;
		this.systems = new ArrayList<>();
		this.children = new ConcurrentHashMap<>();
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
		return new ArrayList<>(children.values());
	}

	/**
	 * Get all the children of the region
	 * @return
	 */
	public List<DistributionRegion> getAllChildren() {
		final List<DistributionRegion> result = new ArrayList<>();

		for(final DistributionRegion child : children.values()) {
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
	 * Get this and all children matching the predicate
	 * @param predicate
	 * @return
	 */
	public List<DistributionRegion> getThisAndChildRegions(final Predicate<DistributionRegion> predicate) {
		final List<DistributionRegion> result = new ArrayList<>();

		getThisAndChildRegions(predicate, result);
		
		return result;
	}
	
	/**
	 * Get this and all children matching the predicate
	 * @param predicate
	 * @return
	 */
	public void getThisAndChildRegions(final Predicate<DistributionRegion> predicate,
			final List<DistributionRegion> result) {	
		
		if(predicate.test(this)) {
			result.add(this);
		}

		final Collection<DistributionRegion> directChildren = children.values();
		
		for(final DistributionRegion children : directChildren) {
			children.getThisAndChildRegions(predicate, result);
		}
	}

	/**
	 * Set the children to state active
	 */
	public void makeChildrenActive() {
		children.values().forEach(c -> c.setState(DistributionRegionState.ACTIVE));
	}

	/**
	 * Merge the distribution group
	 */
	public void merge() {
		children.clear();
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
	 * Get the highest child number
	 */
	public long getHighestChildNumber() {
		return children.keySet().stream().mapToLong(l -> l).max().orElse(0);
	}

	@Override
	public String toString() {
		return "DistributionRegion [distributionGroupName=" + distributionGroupName
				+ ", converingBox=" + converingBox.toCompactString() + ", state=" + state
				+ ", nameprefix=" + regionid + "]";
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
		return getDirectChildren().isEmpty();
	}

	/**
	 * Add a new child
	 * @param newChild
	 */
	public void addChildren(final long childNumber, final DistributionRegion newChild) {

		if(newChild.getParent() != this) {
			throw new IllegalArgumentException("Parent of child " + newChild + " is not this " + this);
		}

		if(children.containsKey(childNumber)) {
			throw new IllegalArgumentException("Child with id " + childNumber + " already exists");
		}

		children.put(childNumber, newChild);
	}

	/**
	 * Get the child with the given number
	 * @param childNumber
	 * @return
	 */
	public DistributionRegion getChildNumber(final long childNumber) {
		return children.get(childNumber);
	}

	/**
	 * Get all known children numbers
	 * @return
	 */
	public List<Long> getAllChildrenNumbers() {
		return new ArrayList<>(children.keySet());
	}

	/**
	 * Has this node children?
	 * @return
	 */
	public boolean hasChildren() {
		return ! children.isEmpty();
	}

	/**
	 * Remove the children
	 */
	public void removeAllChildren() {
		children.clear();
	}

	/**
	 * Remove the children
	 * @return
	 */
	public DistributionRegion removeChildren(final long childrenNumber) {
		return children.remove(childrenNumber);
	}

	/**
	 * Is this the root element?
	 */
	public boolean isRootElement() {
		return (parent == ROOT_NODE_ROOT_POINTER);
	}

	/**
	 * Get the child number
	 * @return
	 */
	public long getChildNumberOfParent() {

		// This is the root element
		if(isRootElement()) {
			return 0;
		}

		return getParent().children.entrySet()
			.stream()
			.filter(e -> e.getValue() == this)
			.map(e -> e.getKey())
			.findAny()
			.orElseThrow(() -> new RuntimeException("Unable to find child number for: " + this));
	}

	/**
	 * Get the covering bounding box
	 * @return
	 */
	public Hyperrectangle getConveringBox() {
		return converingBox;
	}

	/**
	 * Returns get the distribution group name
	 * @return
	 */
	public String getDistributionGroupName() {
		return distributionGroupName;
	}

	/**
	 * Get a unique identifier for this region
	 * @return
	 */
	public String getIdentifier() {
		return distributionGroupName + "_" + regionid;
	}

	/**
	 * Get all systems that are responsible for this DistributionRegion
	 * @return
	 */
	public List<BBoxDBInstance> getSystems() {
		return new ArrayList<>(systems);
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
			systems.clear();
			return;
		}

		final List<BBoxDBInstance> newSystemsList = new ArrayList<>(newSystems.size());
		newSystemsList.addAll(newSystems);

		// Replace systems atomically
		this.systems = newSystemsList;
	}

	/**
	 * Get the region id of the node
	 * @return
	 */
	public long getRegionId() {
		return regionid;
	}
}


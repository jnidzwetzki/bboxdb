package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;

public class DistributionRegion {

	/**
	 * The name of the distribution group
	 */
	protected final DistributionGroupName distributionGroupName;
	
	/**
	 * A pointer that points to the total number of levels
	 */
	protected final TotalLevel totalLevel;
	
	/**
	 * The split position
	 */
	protected float split = Float.MIN_VALUE;
	
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
	protected DistributionRegion parent = null;
	
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
	protected String state = STATE_CREATING;
	
	/**
	 * The systems
	 */
	protected final Collection<DistributedInstance> systems;
	
	/**
	 * The nameprefix of the region
	 */
	protected int nameprefix;
	
	/**
	 * Is the node initialized or not
	 */
	protected volatile boolean ready;
	
	/**
	 * State created
	 */
	public final static String STATE_CREATING = "creating";
	
	/**
	 * State active
	 */
	public final static String STATE_ACTIVE = "active";
	
	/**
	 * State splitting
	 */
	public final static String STATE_SPLITTING = "splitting";
	
	/**
	 * State splitted
	 */
	public final static String STATE_SPLITTED = "splitted";
	
	/**
	 * Protected constructor, the factory method and the set split methods should
	 * be used to create a tree
	 * @param name
	 * @param level
	 */
	protected DistributionRegion(final DistributionGroupName name, final int level, final TotalLevel totalLevel) {
		this.distributionGroupName = name;
		this.level = level;
		this.totalLevel = totalLevel;
		this.converingBox = BoundingBox.createFullCoveringDimensionBoundingBox(name.getDimension());
		
		totalLevel.registerNewLevel(level + 1);
		
		systems = new ArrayList<DistributedInstance>();
		ready = false;
	}

	/**
	 * The node complete event
	 */
	public void onNodeComplete() {
		ready = true;
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
	 * Set the parent of the node
	 * @param parent
	 */
	public void setParent(final DistributionRegion parent) {
		this.parent = parent;
	}
	
	
	/**
	 * Set the split coordinate
	 * @param split
	 */
	public void setSplit(final float split) {
		setSplit(split, true);
	}
	
	/**
	 * Set the split coordinate
	 * @param split
	 */
	public void setSplit(final float split, final boolean sendNotify) {
		this.split = split;
		
		if(leftChild != null || rightChild != null) {
			throw new IllegalArgumentException("Split called, but left or right node are not empty");
		}
		
		leftChild = createNewInstance();
		rightChild = createNewInstance();
		
		leftChild.setParent(this);
		rightChild.setParent(this);
		
		// Calculate the covering bounding boxes
		leftChild.setConveringBox(converingBox.splitAndGetLeft(split, getSplitDimension(), true));
		rightChild.setConveringBox(converingBox.splitAndGetRight(split, getSplitDimension(), false));
		
		afterSplitHook(sendNotify);
		
		// Send the on node complete event
		leftChild.onNodeComplete();
		rightChild.onNodeComplete();
	}

	/**
	 * A hook that is called after the nodes are split
	 * @param sendNotify
	 */
	protected void afterSplitHook(final boolean sendNotify) {
		
	}

	/**
	 * Create a new instance of this type (e.g. for childs)
	 * @return
	 */
	protected DistributionRegion createNewInstance() {
		return new DistributionRegion(distributionGroupName, level + 1, totalLevel);
	}
	
	/**
	 * Get the split coordinate
	 * @return
	 */
	public float getSplit() {
		return split;
	}
	
	/**
	 * Get the name
	 * @return
	 */
	public String getName() {
		return distributionGroupName.getFullname();
	}
	
	/**
	 * Get the state of the node
	 * @return
	 */
	public String getState() {
		return state;
	}

	/**
	 * Set the state of the node
	 * @param state
	 */
	public void setState(final String state) {
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
		return totalLevel.getMaxLevel();
	}
	
	/**
	 * Get the dimension of the distribution region
	 * @return
	 */
	public int getDimension() {
		return distributionGroupName.getDimension();
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
				+ ", systems=" + systems + ", nameprefix=" + nameprefix + ", ready=" + ready + "]";
	}

	/**
	 * Get the root element of the region
	 */
	public DistributionRegion getRootRegion() {
		DistributionRegion currentElement = this;
		
		// Follow the links to the root element
		while(currentElement.getParent() != null) {
			currentElement = currentElement.getParent();
		}
		
		return currentElement;
	}
	
	/**
	 * Is this a leaf region node?
	 * @return
	 */
	public boolean isLeafRegion() {
		if(leftChild != null && rightChild != null) {
			if(leftChild.isReady() && rightChild.isReady()) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Is this the left child of the parent
	 * @return
	 */
	public boolean isLeftChild() {
		if(getParent() == null) {
			return false;
		}
		
		return (getParent().getLeftChild() == this);
	}
	
	/**
	 * Is this the right child of the parent
	 * @return
	 */
	public boolean isRightChild() {
		if(getParent() == null) {
			return false;
		}
		
		return (getParent().getRightChild() == this);
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
	 * Get all systems that are responsible for this DistributionRegion
	 * @return
	 */
	public Collection<DistributedInstance> getSystems() {
		return new ArrayList<DistributedInstance>(systems);
	}
	
	/**
	 * Add a system to this DistributionRegion
	 * @param system
	 */
	public void addSystem(final DistributedInstance system) {
		systems.add(system);
	}
	
	/**
	 * Set the systems for this DistributionRegion
	 * @param systems
	 */
	public void setSystems(final Collection<DistributedInstance> systems) {
		this.systems.clear();
		this.systems.addAll(systems);
	}
	
	/**
	 * Get the a list of systems for the bounding box
	 * @return
	 */
	public List<DistributedInstance> getSystemsForBoundingBox(final BoundingBox boundingBox) {
		final List<DistributedInstance> result = new ArrayList<DistributedInstance>();
		getSystemsForBoundingBoxRecursive(boundingBox, result);
		return result;
	}
	
	/**
	 * Add the leaf nodes systems that are covered by the bounding box
	 * @param boundingBox
	 * @param systems
	 */
	protected void getSystemsForBoundingBoxRecursive(final BoundingBox boundingBox, final Collection<DistributedInstance> resultSystems) {
		
		// This node is not covered. So, edge nodes are not covered
		if(! converingBox.overlaps(boundingBox)) {
			return;
		}
		
		if(isLeafRegion()) {
			for(final DistributedInstance system : systems) {
				if(! resultSystems.contains(system)) {
					resultSystems.add(system);
				}
			}
		} else {
			leftChild.getSystemsForBoundingBoxRecursive(boundingBox, resultSystems);
			rightChild.getSystemsForBoundingBoxRecursive(boundingBox, resultSystems);
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
	 * Is the region ready (initialized)
	 * @return
	 */
	public boolean isReady() {
		return ready;
	}

	/**
	 * Get the nameprefix of the node
	 * @return
	 */
	public int getNameprefix() {
		return nameprefix;
	}

	/**
	 * Set a new nameprefix
	 * @param nameprefix
	 */
	public void setNameprefix(final int nameprefix) {
		this.nameprefix = nameprefix;
	}
	
}

/**
 * A class that represents the max number of levels
 *
 */
class TotalLevel {
	protected int maxLevel = 0;
	
	public void registerNewLevel(final int level) {
		maxLevel = Math.max(maxLevel, level);
	}
	
	public int getMaxLevel() {
		return maxLevel;
	}
}

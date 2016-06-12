package de.fernunihagen.dna.jkn.scalephant.distribution;

import java.util.ArrayList;
import java.util.Collection;

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
	 * The systems
	 */
	protected final Collection<String> systems;
	
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
		
		systems = new ArrayList<String>();
	}

	/**
	 * The node complete event
	 */
	public void onNodeComplete() {
		
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
			throw new IllegalArgumentException("Split called, but left or right node are empty");
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
		return "DistributionNode [name=" + distributionGroupName + ", split=" + split
				+ ", leftChild=" + leftChild + ", rightChild=" + rightChild
				+ ", parent=" + parent + "]";
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
		if(leftChild == null || rightChild == null) {
			return true;
		}
		
		return false;
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
	public Collection<String> getSystems() {
		return new ArrayList<String>(systems);
	}
	
	/**
	 * Add a system to this DistributionRegion
	 * @param system
	 */
	public void addSystem(final String system) {
		systems.add(system);
	}
	
	/**
	 * Set the systems for this DistributionRegion
	 * @param systems
	 */
	public void setSystems(final Collection<String> systems) {
		this.systems.clear();
		this.systems.addAll(systems);
	}
}

/**
 * A class that represents the max number of levels
 *
 */
class TotalLevel {
	protected int maxLevel = 0;
	
	public void registerNewLevel(int level) {
		maxLevel = Math.max(maxLevel, level);
	}
	
	public int getMaxLevel() {
		return maxLevel;
	}
}
